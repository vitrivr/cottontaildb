package ch.unibas.dmi.dbis.cottontail.database.schema

import ch.unibas.dmi.dbis.cottontail.config.Config
import ch.unibas.dmi.dbis.cottontail.database.general.DBO
import ch.unibas.dmi.dbis.cottontail.model.exceptions.DatabaseException

import org.mapdb.*
import org.mapdb.volume.MappedFileVol
import java.io.IOException

import java.lang.ref.SoftReference
import java.nio.file.Files

import java.nio.file.Path
import java.nio.file.Paths
import java.util.*
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.stream.Collectors
import kotlin.concurrent.read

import kotlin.concurrent.write

internal class Schema(override val name: String, config: Config) : DBO {

    /** Resolve the path to the schema. */
    override val path: Path = config.root.resolve( "schema_$name")

    /** The fully qualified name of this [Schema], which is equal to its name. */
    override val fqn: String = name

    /** The fully parent of this [Schema], which is null. */
    override val parent: DBO? = null

    /** Internal reference to the [Store] underpinning this [Column]. */
    private val store: StoreWAL = try {
        StoreWAL.make(file = this.path.resolve(FILE_CATALOGUE).toString(), volumeFactory = MappedFileVol.FACTORY)
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

    /**
     *
     */
    @Volatile
    override var closed: Boolean = false
        private set


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
            header.entities = Arrays.copyOf(header.entities, header.entities.size + 1)
            header.entities[header.entities.size-1] = recId
            this.store.update(HEADER_RECORD_ID, header, SchemaHeaderSerializer)

            /* Initialize new entity; catch exception if any occurs. */
            val entity = Entity.initialize(name, this, *columns)

            /* Commit changes and load entity. */
            this.store.commit()
            this.loaded[name] = SoftReference(entity)
            entity
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
        private const val HEADER_RECORD_ID: Long = 1L

        /** The identifier that is used to identify a Cottontail DB [Schema] file. */
        private const val HEADER_IDENTIFIER: String = "COTTONS"

        /** The version of the Cottontail DB [Schema]  file. */
        private const val HEADER_VERSION: Short = 1

        /** Filename for the [Schema] catalogue.  */
        private const val FILE_CATALOGUE = "catalogue.db"

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
                val store = StoreWAL.make(file = data.resolve(Schema.FILE_CATALOGUE).toString(), volumeFactory = MappedFileVol.FACTORY)
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

    /** The header of the [Schema]. Contains information regarding its content! */
    private class SchemaHeader(val created: Long = System.currentTimeMillis(), var modified: Long = System.currentTimeMillis(), var entities: LongArray = LongArray(0));

    /**
     * The [Serializer] for [SchemaHeader]
     */
    private object SchemaHeaderSerializer: Serializer<SchemaHeader> {
        override fun serialize(out: DataOutput2, value: SchemaHeader) {
            out.writeUTF(Schema.HEADER_IDENTIFIER)
            out.writeShort(Schema.HEADER_VERSION.toInt())
            out.writeLong(value.created)
            out.writeLong(value.modified)
            out.writeInt(value.entities.size)
            for (i in 0 until value.entities.size) {
                out.writeLong(value.entities[i])
            }
        }

        override fun deserialize(input: DataInput2, available: Int): SchemaHeader {
            if (!this.validate(input)) {
                throw DatabaseException.InvalidFileException("Cottontail DB Schema")
            }

            /* Deserialize header. */
            val created = input.readLong()
            val modified = input.readLong()
            val size = input.readInt()
            val entities = LongArray(size)
            for (i in 0 until size) {
                entities[i] = input.readLong()
            }

            /* Return header. */
            return SchemaHeader(created, modified, entities)
        }

        /**
         * Validates the [SchemaHeader]. Must be executed before deserialization
         *
         * @return True if validation was successful, false otherwise.
         */
        private fun validate(input: DataInput2): Boolean {
            val header = input.readUTF()
            val version: Short = input.readShort()
            return (version == Schema.HEADER_VERSION) and (header == Schema.HEADER_IDENTIFIER)
        }
    }
}



