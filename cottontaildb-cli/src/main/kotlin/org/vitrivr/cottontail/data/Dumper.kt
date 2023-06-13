package org.vitrivr.cottontail.data

import kotlinx.serialization.BinaryFormat
import kotlinx.serialization.StringFormat
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.language.basics.Constants.COLUMN_NAME_CLASS
import org.vitrivr.cottontail.client.language.basics.Constants.COLUMN_NAME_DBO
import org.vitrivr.cottontail.client.language.basics.Constants.COLUMN_NAME_NULLABLE
import org.vitrivr.cottontail.client.language.basics.Constants.COLUMN_NAME_SIZE
import org.vitrivr.cottontail.client.language.basics.Constants.COLUMN_NAME_TYPE
import org.vitrivr.cottontail.client.language.ddl.AboutEntity
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.data.Manifest.Companion.MANIFEST_FILE_NAME
import org.vitrivr.cottontail.serialization.valueSerializer
import java.io.Closeable
import java.io.OutputStream
import java.nio.charset.Charset
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
        val serializer = list.firstOrNull()?.valueSerializer() ?: return
        when(val format = this.manifest.format.format) {
            is BinaryFormat -> stream.write(format.encodeToByteArray(ListSerializer(serializer), list))
            is StringFormat -> stream.write(format.encodeToString(ListSerializer(serializer), list).toByteArray())
            else -> throw IllegalArgumentException("Unsupported format $format.")
        }
        stream.flush()
    }

    /**
     * Writes the manifest file to the [OutputStream]. The manifest is always written as JSON!
     *
     * @param stream The [OutputStream] to write to.
     */
    protected fun writeManifest(stream: OutputStream)
        = stream.write(Json.encodeToString(this.manifest).toByteArray(Charset.defaultCharset()))

    /**
     * Internal convenience method to load meta-information about specified [Name.EntityName].
     *
     * @param entity The [Name.EntityName] to load information for.
     */
    protected fun loadEntityInformation(entity: Name.EntityName): Manifest.Entity {
        val results = this.client.about(AboutEntity(entity).txId(this.txId))
        val columns = mutableListOf<ColumnDef<*>>()
        results.forEach {
            if (it.asString(COLUMN_NAME_CLASS) == "COLUMN") {
                columns.add(ColumnDef(
                    Name.ColumnName.parse(it.asString(COLUMN_NAME_DBO)!!),
                    Types.forName(it.asString(COLUMN_NAME_TYPE)!!, it.asInt(COLUMN_NAME_SIZE)!!),
                    it.asBoolean(COLUMN_NAME_NULLABLE)!!
                ))
            }
        }

        return Manifest.Entity(entity, 0L, 0L, columns)
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
            var batch = 0L

            /* Read data and write it to archive. */
            while (results.hasNext()) {
                buffer.add(results.next())
                dumped += 1L
                if (dumped % this.manifest.batchSize == 0L) {
                    Files.newOutputStream(this.output.resolve("${entity.fqn}.${batch}.${this.manifest.format.suffix}"), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE).use {
                        this.writeBatch(buffer,it)
                    }
                    batch += 1
                    buffer.clear()
                }
            }

            /* Write final batch. */
            if (buffer.isNotEmpty()) {
                Files.newOutputStream(this.output.resolve("${entity.fqn}.${batch}.${this.manifest.format.suffix}"), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE).use {
                    this.writeBatch(buffer, it)
                }
                batch += 1
            }

            /* Add manifest entry. */
            (this.manifest.entites as MutableList).add(e.copy(size = dumped, batches = batch))
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
            var batch = 0L

            /* Read data and write it to archive. */
            while (results.hasNext()) {
                buffer.add(results.next())
                dumped += 1L
                if (dumped % this.manifest.batchSize == 0L) {
                    this.stream.putNextEntry(ZipEntry("${entity.fqn}.$batch.${this.manifest.format.suffix}"))
                    this.writeBatch(buffer, this.stream)
                    batch += 1
                    buffer.clear()
                }
            }

            /* Write final batch. */
            if (buffer.isNotEmpty()) {
                this.stream.putNextEntry(ZipEntry("${entity.fqn}.$batch.${this.manifest.format.suffix}"))
                this.writeBatch(buffer, this.stream)
            }

            /* Add manifest entry. */
            (this.manifest.entites as MutableList).add(e.copy(size = dumped, batches = batch))
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