package ch.unibas.dmi.dbis.cottontail.database.schema

import ch.unibas.dmi.dbis.cottontail.config.Config
import ch.unibas.dmi.dbis.cottontail.database.column.Column
import ch.unibas.dmi.dbis.cottontail.database.general.DBO
import ch.unibas.dmi.dbis.cottontail.database.column.ColumnDef
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.model.exceptions.DatabaseException

import org.mapdb.*
import org.mapdb.volume.MappedFileVol
import java.io.IOException

import java.lang.ref.SoftReference
import java.nio.file.Files

import java.nio.file.Path

import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.stream.Collectors

import kotlin.concurrent.read

import kotlin.concurrent.write

/**
 * Represents an schema in the Cottontail DB data model. A [Schema] is a collection of [Entity] objects that belong together
 * (e.g., because they belong to the same application). Every [Schema] can be seen as a dedicated database and different
 * [Schema]s in Cottontail can reside in different locations.
 *
 * Calling the default constructor for [Schema] opens that [Schema]. It can only be opened once due to file locks and it
 * will remain open until the [Schema.close()] method is called.
 *
 * @see Entity
 * @see Column
 *
 * @author Ralph Gasser
 * @version 1.0f
 */
internal class Schema(override val name: String, val config: Config) : DBO {

    /** Resolve the path to the schema. */
    override val path: Path = config.root.resolve( "schema_$name")

    /** The fully qualified name of this [Schema], which is equal to its name. */
    override val fqn: String = name

    /** The fully parent of this [Schema], which is null. */
    override val parent: DBO? = null

    /** Internal reference to the [Store] underpinning this [Column]. */
    private val store: StoreWAL = try {
        StoreWAL.make(file = this.path.resolve(FILE_CATALOGUE).toString(), volumeFactory = MappedFileVol.FACTORY, fileLockWait = config.lockTimeout)
    } catch (e: DBException) {
        throw DatabaseException("Failed to open schema $name at '$path': ${e.message}'")
    }

    /** Reference to the [SchemaHeader] of the [Schema]. */
    private val header
            get() = this.store.get(HEADER_RECORD_ID, SchemaHeaderSerializer) ?: throw DatabaseException.DataCorruptionException("Failed to open header of schema $name!")

    /** A lock used to mediate access to this [Schema]. */
    private val lock = ReentrantReadWriteLock()

    /** Returns a list of [Entity] held by this [Schema]. */
    val entities: List<String>
        get() = header.entities.map { this.store.get(it, Serializer.STRING) ?: throw DatabaseException.DataCorruptionException("Failed to read schema $name ($path): Could not find entity name of ID $it.") }

    /** A map of loaded [Entity] references. The [SoftReference] allow for coping with changing memory conditions. */
    private val loaded = HashMap<String, SoftReference<Entity>>()

    /** Flag indicating whether or not this [Schema] has been closed. */
    @Volatile
    override var closed: Boolean = false
        private set

    /** Size of the [Schema] in terms of [Entity] objects it contains. */
    val size
        get() = this.header.entities.size

    /**
     * Creates a new [Entity] in this [Schema].
     *
     * @param The name of the [Entity] that should be dropped.
     */
    fun createEntity(name: String, vararg columns: ColumnDef) = this.lock.write {
        if (entities.contains(name)) throw DatabaseException("Entity $name.${this.name} cannot be created, because it already exists!")
        try {
            /* Store entry for new entity. */
            val recId = this.store.put(name, Serializer.STRING)

            /* Update schema header. */
            val header = this.header
            header.modified = System.currentTimeMillis()
            header.entities = header.entities.copyOf(header.entities.size + 1)
            header.entities[header.entities.size-1] = recId
            this.store.update(HEADER_RECORD_ID, header, SchemaHeaderSerializer)

            /* Initialize new entity; catch exception if any occurs. */
            Entity.initialize(name, this, *columns)

            /* Commit changes and load entity. */
            this.store.commit()
        } catch (e: Exception) {
            this.store.rollback()
            throw e
        }
    }

    /**
     * Drops an [Entity] in this [Schema]. The act of dropping an [Entity] requires a lock on that [Entity].
     *
     * @param The name of the [Entity] that should be dropped.
     */
    fun dropEntity(name: String) = this.lock.write {
        val entityRecId = this.header.entities.find { this.store.get(it, Serializer.STRING) == name } ?: throw DatabaseException("Entity $name.${this.name} cannot be dropped, because it does not exist!")

        /* Unload the entity and remove it. */
        this.unload(name)

        /* Remove entity. */
        try {
            /* Delete entity name from list. */
            this.store.delete(entityRecId, Serializer.STRING)

            /* Update header. */
            val header = this.header
            header.modified = System.currentTimeMillis()
            header.entities = header.entities.filter { it == entityRecId }.toLongArray()
            this.store.update(HEADER_RECORD_ID, header, SchemaHeaderSerializer)

            /* Commit. */
            this.store.commit()
        } catch (e: DBException) {
            this.store.rollback()
            throw DatabaseException("Entity $name.${this.name} could not be dropped, because of an error in the underlying data store: ${e.message}!")
        }

        /* Delete all files associated with the entity. */
        val pathsToDelete = Files.walk(path.resolve(name)).sorted(Comparator.reverseOrder()).collect(Collectors.toList())
        pathsToDelete.forEach { Files.delete(it) }
    }

    /**
     * Drops the entire [Schema] and deletes all the [Entity] objects that are contained within.
     */
    fun drop() = this.lock.write {
        this.close()

        /* Delete all files associated with the schema. */
        val pathsToDelete = Files.walk(path).sorted(Comparator.reverseOrder()).collect(Collectors.toList())
        pathsToDelete.forEach { Files.delete(it) }
    }

    /**
     * Closes this [Schema] and all the [Entity] objects that are contained within.
     *
     * Since locks to [DBO] or [Transaction] objects may be held by other threads, it can take a
     * while for this method to complete.
     */
    override fun close() = this.lock.write {
        if (!this.closed) {
            this.loaded.entries.removeIf() {
                it.value.get()?.close()
                true
            }
            this.store.close()
        }
    }

    /**
     * Returns an instance of [Entity] if such an instance exists. If the [Entity] has been loaded before, that [Entity] is re-used.
     * Otherwise, the [Entity] will be loaded from disk.
     */
    fun get(name: String): Entity = this.lock.read {
        var entity = this.loaded[name]?.get()
        if (entity != null) {
            return entity
        } else {
            if (!entities.contains(name)) throw DatabaseException("Entity $name.${this.name} cannot be opened, because it does not exists!")
            entity = Entity(name, this)
            this.loaded[name] = SoftReference(entity)
            return entity
        }
    }

    /**
     * Explicitly unloads (closes) an instance of [Entity] if such an instance exists. Otherwise, this function has now effect.
     */
    private fun unload(name: String) = this.lock.write {
        this.loaded[name]?.get()?.close() /* BLOCK! */
        this.loaded.remove(name)
    }

    /**
     * Companion object with different constants.
     */
    companion object {
        /** ID of the schema header! */
        internal const val HEADER_RECORD_ID: Long = 1L

        /** Filename for the [Schema] catalogue.  */
        internal const val FILE_CATALOGUE = "catalogue.db"

        /** */
        fun create(name: String, config: Config): Schema {
            /* Create empty folder for entity. */
            val data = config.root.resolve("schema_$name")
            try {
                if (!Files.exists(data)) {
                    Files.createDirectories(data)
                } else {
                    throw DatabaseException("Failed to create schema '$name'. Data directory '$data' seems to be occupied.")
                }
            } catch (e: IOException) {
                throw DatabaseException("Failed to create schema '${name}' due to an IO exception: {${e.message}")
            }

            /* Generate the store. */
            try {
                val store = StoreWAL.make(file = data.resolve(Schema.FILE_CATALOGUE).toString(), volumeFactory = MappedFileVol.FACTORY, fileLockWait = config.lockTimeout)
                store.preallocate() /* Pre-allocates the header. */
                store.update(Schema.HEADER_RECORD_ID, SchemaHeader(), SchemaHeaderSerializer)
                store.commit()
                store.close()
                return Schema(name, config)
            } catch (e: DBException) {
                val pathsToDelete = Files.walk(data).sorted(Comparator.reverseOrder()).collect(Collectors.toList())
                pathsToDelete.forEach { Files.delete(it) }
                throw DatabaseException("Failed to create schema '$name' due to a storage exception: {${e.message}")
            }
        }
    }
}



