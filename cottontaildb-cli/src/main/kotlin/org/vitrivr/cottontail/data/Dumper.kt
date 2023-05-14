package org.vitrivr.cottontail.data

import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.cbor.Cbor
import kotlinx.serialization.csv.Csv
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.encodeToStream
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.iterators.Tuple
import org.vitrivr.cottontail.client.language.basics.Constants.COLUMN_NAME_CLASS
import org.vitrivr.cottontail.client.language.basics.Constants.COLUMN_NAME_DBO
import org.vitrivr.cottontail.client.language.basics.Constants.COLUMN_NAME_NULLABLE
import org.vitrivr.cottontail.client.language.basics.Constants.COLUMN_NAME_ROWS
import org.vitrivr.cottontail.client.language.basics.Constants.COLUMN_NAME_SIZE
import org.vitrivr.cottontail.client.language.basics.Constants.COLUMN_NAME_TYPE
import org.vitrivr.cottontail.client.language.ddl.AboutEntity
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.data.Manifest.Companion.MANIFEST_FILE_NAME
import org.vitrivr.cottontail.serialization.valueSerializer
import java.io.Closeable
import java.io.OutputStream
import java.lang.Math.floorDiv
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

/**
 * A class that can be used to dump entities into a consistent snapshot (dump).
 *
 * [Dumper]s are used as follows:
 * - Create [Dumper] instance with a specified output [Path] and [Format]
 * - Dump individual entities using [Dumper.dump]
 * - Close the [Dumper] (important, otherwise [Manifest] will not be persisted).
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class Dumper(protected val client: SimpleClient, protected val output: Path, format: Format, batchSize: Int = 100000): Closeable {
    /** The [Manifest] of this [Dumper]. Keeps track of entities, that have been dumped. */
    val manifest: Manifest = Manifest(format, batchSize)

    /** Dumping always takes place in a single read-only transaction, so we get a consistent snapshot. */
    protected val txId = this.client.begin(true)

    /** A flag indicating, whether this [Dumper] was closed.*/
    protected var closed = false

    /**
     * Abstract function that dumps the specified [Name.EntityName] into the database dump managed by this [Dumper] instance.
     *
     * @param entity The [Name.EntityName] to dump.
     * @return The number of elements that were dumped.
     */
    abstract fun dump(entity: Name.EntityName): Long

    /**
     * Writes the manifest file to the [OutputStream].
     *
     * @param stream The [OutputStream] to write to.
     */
    protected fun writeBatch(list: List<Tuple>, stream: OutputStream) {
        when(this.manifest.format) {
            Format.CBOR -> {
                val serializer = list.firstOrNull()?.valueSerializer() ?: return
                stream.write(Cbor.encodeToByteArray(ListSerializer(serializer), list))
            }
            Format.JSON -> {
                val serializer = list.firstOrNull()?.valueSerializer() ?: return
                Json.encodeToStream(ListSerializer(serializer), list, stream)
            }
            Format.CSV -> {
                val serializer = list.firstOrNull()?.valueSerializer() ?: return
                stream.write(Csv.encodeToString(ListSerializer(serializer), list).toByteArray())
            }
        }
        stream.flush()
    }

    /**
     * Writes the manifest file to the [OutputStream]. The manifest is always written as JSON!
     *
     * @param stream The [OutputStream] to write to.
     */
    protected fun writeManifest(stream: OutputStream) = Json.encodeToStream(this.manifest, stream)

    /**
     * Internal convenience method to load meta-information about specified [Name.EntityName].
     *
     * @param entity The [Name.EntityName] to load information for.
     */
    protected fun loadEntityInformation(entity: Name.EntityName): Manifest.Entity {
        val results = this.client.about(AboutEntity(entity).txId(this.txId))
        val columns = mutableListOf<ColumnDef<*>>()
        var count: Long = 0L
        results.forEach {
            if (it.asString(COLUMN_NAME_CLASS) == "COLUMN") {
                columns.add(ColumnDef(
                    Name.ColumnName.parse(it.asString(COLUMN_NAME_DBO)!!),
                    Types.forName(it.asString(COLUMN_NAME_TYPE)!!, it.asInt(COLUMN_NAME_SIZE)!!),
                    it.asBoolean(COLUMN_NAME_NULLABLE)!!
                ))
            }
            if (it.asString(COLUMN_NAME_CLASS) == "ENTITY") {
                count = it.asLong(COLUMN_NAME_ROWS)!!
            }
        }

        val batches = if (count % this.manifest.batchSize > 0) {
            floorDiv(count, this.manifest.batchSize) + 1
        } else {
            floorDiv(count, this.manifest.batchSize)
        }
        return Manifest.Entity(entity, batches, count, columns)
    }

    /**
     * Ends the transaction used for dumping.
     */
    override fun close() {
        if (!this.closed) {
            this.client.rollback(this.txId)
            this.closed = true
        }
    }

    /**
     * A [Dumper] for the folder-based storage layout.
     */
    class Folder(client: SimpleClient, output: Path, format: Format, batchSize: Int): Dumper(client, output, format, batchSize)  {
        init {
            if (!Files.exists(this.output)) {
                Files.createDirectories(this.output)
            }
            require(Files.isDirectory(this.output)) { "Cannot dump to ${this.output}: Parent is not a directory."}
        }

        /**
         * Dumps the specified [Name.EntityName] into the database dump managed by this [Dumper] instance.
         *
         * @param entity The [Name.EntityName] to dump.
         * @return The number of elements that were dumped.
         */
        override fun dump(entity: Name.EntityName): Long {
            /* Load basic entity information. */
            val e = loadEntityInformation(entity)

            /* Start dumping the entity in batches. */
            val buffer = mutableListOf<Tuple>()
            val results = this.client.query(Query(entity).txId(this.txId))
            var dumped = 0L
            for (i in 0L until e.batches) {
                buffer.clear()
                Files.newOutputStream(this.output.resolve("${entity.fqn}.${i}.${this.manifest.format.suffix}"), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE).use {
                    var j = 0
                    while (results.hasNext()) {
                        buffer.add(results.next())
                        dumped += 1L
                        if (j++ == this.manifest.batchSize) {
                            break
                        }
                    }
                    this.writeBatch(buffer, it)
                }
            }

            /* Add manifest entry. */
            (this.manifest.entites as MutableList).add(e)
            return dumped
        }

        /**
         * Closes this [Dumper.Zip].
         */
        override fun close() {
            if (!this.closed) {
                Files.newOutputStream(this.output.resolve(MANIFEST_FILE_NAME)).use {
                    writeManifest(it)
                }
                super.close()
            }
        }
    }

    /**
     * A [Dumper] for the ZIP-file-based storage layout.
     */
    class Zip(client: SimpleClient, output: Path, format: Format, batchSize: Int): Dumper(client, output, format, batchSize) {

        /** The output stream used by this [Zip]. */
        private val stream: ZipOutputStream

        init {
            require(!Files.exists(this.output)) { "Cannot dump to ${this.output}: File already exists!"}
            require(Files.isDirectory(this.output.parent)) { "Cannot dump to ${this.output}: Parent is not a directory."}
            this.stream = ZipOutputStream(Files.newOutputStream(this.output, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE))
        }

        /**
         * Dumps the specified [Name.EntityName] into the database dump managed by this [Dumper] instance.
         *
         * @param entity The [Name.EntityName] to dump.
         * @return The number of elements that were dumped.
         */
        override fun dump(entity: Name.EntityName): Long {
            /* Load basic entity information. */
            val e = loadEntityInformation(entity)

            /* Start dumping the entity in batches. */
            val buffer = mutableListOf<Tuple>()
            val results = this.client.query(Query(entity).txId(this.txId))
            var dumped = 0L
            for (i in 0L until e.batches) {
                buffer.clear()
                this.stream.putNextEntry(ZipEntry("${entity.fqn}.${i}.${this.manifest.format.suffix}"))
                var j = 0
                while (results.hasNext()) {
                    buffer.add(results.next())
                    dumped += 1L
                    if (j++ == this.manifest.batchSize) {
                        break
                    }
                }
                this.writeBatch(buffer, this.stream)
            }

            /* Add manifest entry. */
            (this.manifest.entites as MutableList).add(e)
            return dumped
        }

        /**
         * Closes this [Dumper.Zip].
         */
        override fun close() {
            if (!this.closed) {
                /* Write manifest. */
                this.stream.putNextEntry(ZipEntry(MANIFEST_FILE_NAME))
                this.writeManifest(this.stream)
                this.stream.close()
                super.close()
            }
        }
    }
}