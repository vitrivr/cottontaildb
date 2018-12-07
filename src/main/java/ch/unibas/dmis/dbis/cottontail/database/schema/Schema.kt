package ch.unibas.dmis.dbis.cottontail.database.schema

import ch.unibas.dmis.dbis.cottontail.config.Config
import ch.unibas.dmis.dbis.cottontail.database.definition.EntityDefinition
import ch.unibas.dmis.dbis.cottontail.model.DatabaseException
import ch.unibas.dmis.dbis.cottontail.model.LockedException
import ch.unibas.dmis.dbis.cottontail.serializer.schema.Serializers
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.mapdb.DBMaker
import org.mapdb.Serializer

import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.util.stream.Collectors
import kotlin.Comparator

class Schema
/**
 * Constructor for [Schema].
 *
 * @param config [Config] from which to create [Schema].
 * @throws IOException
 */
@Throws(IOException::class)
constructor(config: Config) {

    /**
     * List of [Entity]s contained in this [Schema]. Changes are synced to disk. However, the complete
     * catalogue remains in memory all of the time.
     */
    private val entities = HashMap<String, Entity>()

    /** A lock that mediates access to the [Schema] and its [Entity].  */
    private val lock = ReentrantReadWriteLock()

    /** Folder that contains all the data for this [Schema].  */
    private val dataFolder: Path

    /** The maximum time to wait when acquiring locks in milliseconds.  */
    private val lock_timeout_ms: Int

    init {
        this.dataFolder = Paths.get(config.dataFolder)
        this.lock_timeout_ms = config.lockTimeout

        /* Create folder (if it doesn't exist). */
        if (!Files.exists(this.dataFolder)) {
            Files.createDirectories(this.dataFolder)
        }

        /* Open / create the catalogue and its entries. */
        val catalogue = DBMaker.fileDB(this.dataFolder.resolve(FILE_CATALOGUE).toFile()).fileMmapEnableIfSupported().make()
        val entities = catalogue.hashMap(PROPERTY_ENTITIES)
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializers.ENTITY_DEF_SERIALIZER)
                .createOrOpen()

        /* Load entities from catalogue. */
        for ((key, value) in entities) {
            this.entities[key] = Entity(value, this.lock_timeout_ms)
        }

        /* Update basic properties. */
        val now = System.currentTimeMillis()
        catalogue.atomicLong(PROPERTY_LAST_OPEN, now).createOrOpen().set(now)
        catalogue.atomicLong(PROPERTY_LAST_MODIFIED, now).createOrOpen()
        catalogue.atomicLong(PROPERTY_CREATED, now).createOrOpen()
        catalogue.atomicLong(PROPERTY_VERSION, 0).createOrOpen()

        /* Close catalogue. */
        catalogue.close()
    }

    /**
     * Creates a new [Entity] and adds it to this [Schema].
     *
     * @param name Name of the [Entity] to create.
     * @return [Entity] that was created.
     *
     * @throws LockedException If no lock could be acquired for the catalogue. This means, that you should re-try later.
     * @throws DatabaseException If creating the [Entity] failed due to some database issue.
     */
    @Throws(DatabaseException::class)
    fun createEntity(name: String): Entity {
        try {
            if (this.lock.writeLock().tryLock(this.lock_timeout_ms.toLong(), TimeUnit.MILLISECONDS)) { /* Acquire write-lock on schema. */
                try {
                    /* Check if entity with that name exists. */
                    if (this.entities.containsKey(name)) {
                        throw DatabaseException("Failed to create entity. An entity named '%s' already exists in this schema.", name)
                    }

                    /* Make changes to catalogue. */
                    try {
                        DBMaker.fileDB(this.dataFolder.resolve(FILE_CATALOGUE).toFile()).transactionEnable().fileMmapEnableIfSupported().make().use { catalogue ->
                            /* Create EntityDefinition and Entity instance. */
                            val def = EntityDefinition(name, this.dataFolder.resolve(name))
                            val entity = Entity(def, this.lock_timeout_ms)

                            /* Update catalogue. */
                            catalogue.hashMap(PROPERTY_ENTITIES).keySerializer(Serializer.STRING).valueSerializer(Serializers.ENTITY_DEF_SERIALIZER).open()[name] = def
                            catalogue.atomicLong(PROPERTY_LAST_MODIFIED).open().set(System.currentTimeMillis())
                            catalogue.atomicLong(PROPERTY_VERSION).open().incrementAndGet()
                            catalogue.commit()

                            /* Update local state. */
                            this.entities[name] = entity
                            return entity
                        }
                    } catch (e: Exception) {
                        LOGGER.log(Level.ERROR, "Failed to create entity '{}'. Could not update catalogue due to exception: {}", name, e)
                        throw DatabaseException("Failed to create entity '%s'. Could not update catalogue due to exception.", name)
                    }

                } finally {
                    this.lock.writeLock().unlock() /* Relinquish write-lock on schema. */
                }
            } else {
                throw LockedException("Failed to create entity '%s'. Could not acquire write-lock on catalogue. Timeout of %dms has elapsed.", name, this.lock_timeout_ms)
            }
        } catch (e: InterruptedException) {
            throw LockedException("FFailed to create entity '%s'. Could not acquire write-lock on catalogue. Thread was interrupted while waiting for lock to become free.", name)
        }

    }

    /**
     * Drops the [Entity] for the given name from this [Schema].
     *
     * @param name Name of the [Entity] to drop.
     *
     * @throws LockedException If no lock could be acquired for the catalogue. This means, that you should re-try later.
     * @throws DatabaseException If dropping the [Entity] failed due to some database issue.
     */
    @Throws(DatabaseException::class)
    fun dropEntity(name: String) {
        try {
            if (this.lock.writeLock().tryLock(this.lock_timeout_ms.toLong(), TimeUnit.MILLISECONDS)) { /* Acquire write-lock on schema. */
                try {
                    /* Unmount entity from this catalogue. */
                    val entity = this.entities.remove(name)
                            ?: throw DatabaseException("Failed to remove entity. The entity '%s' does not exists in schema.", name)

                    /* Make changes to catalogue. */
                    try {
                        DBMaker.fileDB(this.dataFolder.resolve(FILE_CATALOGUE).toFile()).transactionEnable().fileMmapEnableIfSupported().make().use { catalogue ->
                            catalogue.hashMap(PROPERTY_ENTITIES).keySerializer(Serializer.STRING).valueSerializer(Serializers.ENTITY_DEF_SERIALIZER).open().remove(name)
                            catalogue.atomicLong(PROPERTY_LAST_MODIFIED).open().set(System.currentTimeMillis())
                            catalogue.atomicLong(PROPERTY_VERSION).open().incrementAndGet()
                            catalogue.commit()
                        }
                    } catch (e: Exception) {
                        this.entities[name] = entity
                        LOGGER.log(Level.ERROR, "Failed to drop entity '{}'. Could not update catalogue due to exception: {}", name, e)
                        throw DatabaseException("Failed to drop entity '%s'. Could not update catalogue due to unknown exception.", name)
                    }

                    /* Delete files associated with entity. */
                    try {
                        val files = Files.walk(entity.path).sorted(Comparator.reverseOrder()).collect(Collectors.toList())
                        for (p in files) {
                            Files.deleteIfExists(p)
                        }
                    } catch (e: IOException) {
                        LOGGER.log(Level.WARN, "Could not delete files associated with entity '{}' due to an IOException: {}", name, e)
                    }

                } finally {
                    this.lock.writeLock().unlock() /* Relinquish write-lock on schema. */
                }
            } else {
                throw LockedException("Failed to drop entity '%s'. Could not acquire write-lock on catalogue. Timeout of %dms has elapsed.", name, this.lock_timeout_ms)
            }
        } catch (e: InterruptedException) {
            throw LockedException("Failed to drop entity '%s'. Could not acquire write-lock on catalogue. Thread was interrupted while waiting for lock to become free.", name)
        }
    }

    /**
     * Returns the [Entity] associated with the provided name [Schema].
     *
     * @param name The name of the [Entity] to return.
     * @return Optional [Entity] associated with the given name.
     */
    fun entityForName(name: String): Entity? {
        this.lock.readLock().tryLock()
        val entity = this.entities[name]
        this.lock.readLock().unlock()
        return entity
    }

    /**
     * Returns a list of [Entity] names that are contained in this [Schema]. Mind, that the list can
     * be outdated the moment it is returned.
     *
     * @return List of [Entity] names.
     */
    fun entities(): Collection<String> {
        this.lock.readLock().tryLock()
        val entities = Collections.unmodifiableCollection(this.entities.keys)
        this.lock.readLock().unlock()
        return entities
    }

    companion object {
        /** Filename for the [Schema] catalogue.  */
        private val FILE_CATALOGUE = "catalogue.mapdb"

        /** Filename for the [Schema] catalogue.  */
        private val PROPERTY_ENTITIES = "entities"

        /** Property name for the [Schema] version.  */
        private val PROPERTY_VERSION = "version"

        /** Property name for the last time this [Schema] was opened.  */
        private val PROPERTY_LAST_OPEN = "opened"

        /** Property name for the last time this [Schema] modified.  */
        private val PROPERTY_LAST_MODIFIED = "modified"

        /** Property name for the date this [Schema] created.  */
        private val PROPERTY_CREATED = "created"

        /** The [Logger] instance used to log errors and information.  */
        private val LOGGER = LogManager.getLogger()
    }
}
