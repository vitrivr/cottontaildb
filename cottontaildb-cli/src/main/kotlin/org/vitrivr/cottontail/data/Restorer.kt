package org.vitrivr.cottontail.data

import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.cbor.Cbor
import kotlinx.serialization.csv.Csv
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.decodeFromStream
import org.vitrivr.cottontail.client.SimpleClient
import org.vitrivr.cottontail.client.iterators.Tuple
import org.vitrivr.cottontail.client.language.ddl.CreateEntity
import org.vitrivr.cottontail.client.language.dml.BatchInsert
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.serialization.valueSerializer
import java.io.Closeable
import java.io.InputStream
import java.nio.charset.Charset
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import java.util.*
import java.util.zip.ZipFile

/**
 * A class that can be used to restore entities dumped by the [Dumper] class.
 *
 * [Restorer]s are used as follows:
 * - Create [Restorer] instance with a specified input [Path].
 * - Access [Manifest] to read entities contained.
 * - Restore individual entities using [Restorer.restore]
 * - Close the [Restorer]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class Restorer(protected val client: SimpleClient, protected val output: Path): Closeable {

    /** The [Manifest] of this [Restorer]. Keeps track of entities, that should be restored. Is read from the dump. */
    abstract val manifest: Manifest

    /** A flag indicating, whether this [Dumper] was closed.*/
    protected var closed = false

    /**
     * Creates an [Iterator] of [Tuple] for the [Name.EntityName] held by this [Restorer].
     *
     * @param entity [Name.EntityName]
     * @return [Iterator] of [Tuple]s
     */
    fun iterator(entity: Manifest.Entity): Iterator<Tuple>  {
        val e = this.manifest.entites.find { it == entity } ?: throw IllegalArgumentException("Could not find entry for entity $entity in database dump.")
        return object: Iterator<Tuple> {
            private var batchIndex = 0L
            private val buffer = LinkedList<Tuple>()
            override fun hasNext(): Boolean {
                if (this.buffer.isNotEmpty()) return true
                if (this.batchIndex < entity.batches) {
                    this.buffer.addAll(this@Restorer.loadBatch(e, this.batchIndex++))
                }
                return this.buffer.isNotEmpty()
            }
            override fun next(): Tuple = this.buffer.poll()
        }
    }

    /**
     * Tries to restore an entity from a database dump.
     *
     * @param entity [Manifest.Entity]
     */
    fun restore(entity: Manifest.Entity) {
        val tuples = this.iterator(entity)
        val txId = this.client.begin(false)
        try {
            /* Create entity. */
            val create = CreateEntity(entity.name).txId(txId)
            for (c in entity.columns) {
                create.column(c)
            }
            this.client.create(create)

            /* Insert the data. */
            val insert = BatchInsert(entity.name).columns(*entity.columns.map { it.name }.toTypedArray()).txId(txId)
            for (t in tuples) {
                if (!insert.values(*t.values.toTypedArray())) {
                    this.client.insert(insert)
                    insert.clear()
                }
            }
            if (insert.count() > 0) {
                this.client.insert(insert)
            }

            /* Commit. */
            this.client.commit(txId)
        } catch (e: Throwable) {
            this.client.rollback(txId)
            throw e
        }
    }

    /**
     * Reads a batch if [Tuple] from the provided [InputStream].
     *
     * @param e The [Manifest.Entity] to read data for.
     * @param input The [InputStream] to read from.
     * @return [List] of [Tuple]
     */
    protected fun read(e: Manifest.Entity, input: InputStream): List<Tuple> {
        val serializer = e.columns.valueSerializer()
        val bytes = input.readAllBytes()
        return when(this.manifest.format) {
            Format.CBOR -> Cbor.decodeFromByteArray(ListSerializer(serializer), bytes)
            Format.JSON -> Json.decodeFromString(ListSerializer(serializer), bytes.toString(Charset.defaultCharset()))
            Format.CSV -> Csv.decodeFromString(ListSerializer(serializer), bytes.toString(Charset.defaultCharset()))
        }
    }

    /**
     * Closes this [Restorer]
     */
    override fun close() {
        if (!this.closed) {
            this.closed = true
        }
    }

    /**
     * This abstract function loads a batch of [Tuple] from the dump.
     *
     * @param e The [Manifest.Entity] to restore.
     * @param batchIndex The index of the batch to load.
     * @return [List] of [Tuple]
     */
    abstract fun loadBatch(e: Manifest.Entity, batchIndex: Long): List<Tuple>

    /**
     * A [Restorer] for the folder-based storage layout.
     */
    class Folder(client: SimpleClient, output: Path): Restorer(client, output)  {
        /** The [Manifest] of this [Dumper]. Keeps track of entities, that have been dumped. */
        override val manifest: Manifest

        init {
            this.manifest = try {
                Files.newInputStream(output.resolve(Manifest.MANIFEST_FILE_NAME), StandardOpenOption.READ).use { Json.decodeFromStream(it) }
            } catch (e: Throwable) {
                throw IllegalArgumentException("Unable to restore dump: Failed to read MANIFEST from ${this.output}.")
            }
        }

        /**
         * This function loads a batch of [Tuple]s from the dump by reading the respective ZIP entry.
         *
         * @param e The [Entity] to restore.
         * @param batchIndex The index of the batch to load.
         * @return [List] of [Tuple]s read.
         */
        override fun loadBatch(e: Manifest.Entity, batchIndex: Long): List<Tuple> {
            return Files.newInputStream(output.resolve("${e.name.fqn}.${batchIndex}.${this.manifest.format.suffix}"), StandardOpenOption.READ).use {
                this.read(e, it)
            }
        }
    }


    /**
     * A [Restorer] for the ZIP-file-based storage layout.
     */
    class Zip(client: SimpleClient, input: Path): Restorer(client, input) {
        /** Opens the [ZipFile]. */
        private val zip: ZipFile

        /** The [Manifest] of this [Dumper]. Keeps track of entities, that have been dumped. */
        override val manifest: Manifest

        init {
            this.zip = try {
                ZipFile(input.toFile(), ZipFile.OPEN_READ)
            } catch (e: Throwable) {
                throw IllegalArgumentException("Unable to restore dump: Failed to read ZIP file: ${this.output}.")
            }

            this.manifest = try {
                val entry = this.zip.getEntry(Manifest.MANIFEST_FILE_NAME)
                this.zip.getInputStream(entry).use {
                    Json.decodeFromStream(it)
                }
            } catch (e: Throwable) {
                throw IllegalArgumentException("Unable to restore dump: Failed to read MANIFEST from ${this.output}.")
            }
        }

        /**
         * This function loads a batch of [Tuple]s from the dump by reading the respective ZIP entry.
         *
         * @param e The [Manifest.Entity] to restore.
         * @param batchIndex The index of the batch to load.
         * @return [List] of [Tuple]s read.
         */
        override fun loadBatch(e: Manifest.Entity, batchIndex: Long): List<Tuple> {
            val entry = this.zip.getEntry("${e.name.fqn}.${batchIndex}.${this.manifest.format.suffix}")
            return this.zip.getInputStream(entry).use {
                this.read(e, it)
            }
        }

        /**
         * Closes this [Restorer]
         */
        override fun close() {
            if (!this.closed) {
                this.zip.close()
                super.close()
            }
        }
    }
}
