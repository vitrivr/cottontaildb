package ch.unibas.dmi.dbis.cottontail.database.catalogue

import ch.unibas.dmi.dbis.cottontail.config.Config
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.DBO
import ch.unibas.dmi.dbis.cottontail.database.schema.Schema
import ch.unibas.dmi.dbis.cottontail.database.schema.SchemaHeader
import ch.unibas.dmi.dbis.cottontail.database.schema.SchemaHeaderSerializer
import ch.unibas.dmi.dbis.cottontail.model.exceptions.DatabaseException
import org.mapdb.*

import org.mapdb.volume.MappedFileVol
import java.io.IOException
import java.net.InetAddress
import java.nio.file.Files

import java.nio.file.Path
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.stream.Collectors
import kotlin.concurrent.read
import kotlin.concurrent.write


/**
 * The main catalogue in Cottontail DB. It contains references to all the [Schema]s managed by Cottontail
 * and is the main way of accessing these [Schema]s and creating new ones.
 *
 * @see Schema
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class Catalogue(val config: Config): DBO {
    /** Root to Cottontail DB root folder. */
    override val path: Path = config.root

    /** Constant name of the [Catalogue] object. */
    override val name: String = "cottontail@${InetAddress.getLocalHost().hostName}"

    /** Constant parent [DBO], which is null in case of the [Catalogue]. */
    override val parent: DBO? = null

    /** A lock used to mediate access to this [Catalogue]. */
    private val lock = ReentrantReadWriteLock()

    /** A in-memory registry of all the [Schema]s contained in this [Catalogue]. When a [Catalogue] is opened, all the [Schema]s will be loaded. */
    private val registry: HashMap<String, Schema> = HashMap()

    /** The [StoreWAL] that contains the Cottontail DB catalogue. */
    private val store: StoreWAL = try {
        StoreWAL.make(file = this.path.resolve(FILE_CATALOGUE).toString(), volumeFactory = MappedFileVol.FACTORY, fileLockWait = config.lockTimeout)
    } catch (e: DBException) {
        throw DatabaseException("Failed to open Cottontail DB catalogue: ${e.message}'.")
    }

    /** Reference to the [CatalogueHeader] of the [Catalogue]. Accessing it will read right from the underlying store. */
    private val header: CatalogueHeader
        get() = this.store.get(HEADER_RECORD_ID, CatalogueHeaderSerializer) ?: throw DatabaseException.DataCorruptionException("Failed to open Cottontail DB catalogue header!")

    /** Size of this [Catalogue] in terms of [Schema]s it contains. */
    val size: Int
        get() = this.lock.read { this.header.schemas.size }

    /** Status indicating whether this [Catalogue] is open or closed. */
    @Volatile
    override var closed: Boolean = false
        private set

    /**
     * Closes the [Catalogue] and all objects contained within.
     */
    override fun close() = this.lock.write {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    /** Initialization logic for [Catalogue]. */
    init {
        val header = this.header
        for (sid in header.schemas) {
            val schema = this.store.get(sid, CatalogueEntrySerializer) ?: throw DatabaseException.DataCorruptionException("Failed to open Cottontail DB catalogue entry!")
            if (!Files.exists(schema.path)) {
                throw DatabaseException.DataCorruptionException("Broken catalogue entry ${schema.name} (${schema.path}). Schema does not exist!")
            }
            this.registry[schema.name] = Schema(schema.name, schema.path, this)
        }
    }

    /**
     * Creates a new, empty [Schema] with the given name and [Path]
     *
     * @param name The name of the new [Schema].
     * @param data The path where this new [Schema] will be located. Defaults to a path relative to the current one.
     */
    fun createSchema(name: String, data: Path = this.path.resolve("schema_$name")) = this.lock.write {
        /* Check if schema with that name exists. */
        if (this.registry.containsKey(name)) {
            throw DatabaseException("Failed to create schema '$name'. Schema with that name already exists.")
        }

        /* Create empty folder for entity. */
        try {
            if (!Files.exists(data)) {
                Files.createDirectories(data)
            } else {
                throw DatabaseException("Failed to create schema '$name'. Data directory '$data' seems to be occupied.")
            }
        } catch (e: IOException) {
            throw DatabaseException("Failed to create schema '$name' due to an IO exception: ${e.message}")
        }

        /* Generate the store for the new schema and update catalogue. */
        try {
            /* Create new store. */
            val store = StoreWAL.make(file = data.resolve(Schema.FILE_CATALOGUE).toString(), volumeFactory = MappedFileVol.FACTORY, fileLockWait = config.lockTimeout)
            store.put(SchemaHeader(), SchemaHeaderSerializer)
            store.commit()
            store.close()

            /* Update catalogue. */
            val sid = this.store.put(CatalogueEntry(name, data), CatalogueEntrySerializer)

            /* Update header. */
            val new = this.header.let { CatalogueHeader(it.size + 1, it.created, System.currentTimeMillis(), it.schemas.copyOf(it.schemas.size + 1)) }
            new.schemas[new.schemas.size-1] = sid
            this.store.update(HEADER_RECORD_ID, new, CatalogueHeaderSerializer)
            this.store.commit()
        } catch (e: DBException) {
            val pathsToDelete = Files.walk(data).sorted(Comparator.reverseOrder()).collect(Collectors.toList())
            pathsToDelete.forEach { Files.delete(it) }
            throw DatabaseException("Failed to create schema '$name' due to a storage exception: ${e.message}")
        }

        /* Add schema to local map. */
        this.registry[name] = Schema(name, path, this)
    }

    /**
     * Drops an existing [Schema] with the given name. <strong>Warning:</strong> Dropping a [Schema] deletes all the files associated with it [Schema]!
     *
     * @param name The name of the [Schema] to be dropped.
     */
    fun dropSchema(name: String) = this.lock.write {
        /* Try to close the schema. Open registry cannot be dropped. */
        (this.registry[name] ?: throw DatabaseException("Failed to drop schema '$name'. Schema does not exist.")).close()

        /* Extract the catalogue entry. */
        val catalogueEntry = this.header.schemas
                .map { Pair(it, this.store.get(it, CatalogueEntrySerializer) ?: throw DatabaseException.DataCorruptionException("Failed to read Cottontail DB catalogue entry for SID=$it!")) }
                .find { it.second.name == name } ?: throw DatabaseException("Failed to drop schema '$name'. Did not find a Cottontail DB catalogue entry for schema $name!")

        /* Remove catalogue entry + update header. */
        try {
            this.store.delete(catalogueEntry.first, CatalogueEntrySerializer)
            val new = this.header.let { CatalogueHeader(it.size - 1, it.created, System.currentTimeMillis(), it.schemas.filter { it != catalogueEntry.first }.toLongArray()) }
            this.store.update(HEADER_RECORD_ID, new, CatalogueHeaderSerializer)
            this.store.commit()
        } catch (e: DBException) {
            throw DatabaseException("Failed to dop schema '$name' due to a storage exception: ${e.message}")
        }

        /* Remove schema from registry. */
        this.registry.remove(name)

        /* Delete files that belong to the schema. */
        val pathsToDelete = Files.walk(catalogueEntry.second.path).sorted(Comparator.reverseOrder()).collect(Collectors.toList())
        pathsToDelete.forEach { Files.delete(it) }
    }

    /**
     * Returns a list of [Schema] names for this [Catalogue].
     *
     * @return The list of [Schema] names registered in this [Catalogue]
     */
    fun listSchemas(): List<String> = this.lock.read { this.registry.keys.toList() }

    /**
     * Returns the [Schema] for the given name.
     *
     * @param name Name of the [Schema].
     */
    fun getSchema(name: String): Schema = this.lock.read { registry[name] ?: throw DatabaseException("Schema $name cannot be opened, because it does not exists!") }

    /**
     * Returns true, if this [Catalogue] contains a [Schema] with the provided name.
     *
     * @param name Name of the [Schema].
     */
    fun hasSchema(name: String) = this.lock.read { this.registry.containsKey(name) }

    /**
     * Companion object to [Catalogue]
     */
    companion object {
        /** ID of the schema header! */
        internal const val HEADER_RECORD_ID: Long = 1L

        /** Filename for the [Entity] catalogue.  */
        internal const val FILE_CATALOGUE = "catalogue.db"
    }
}