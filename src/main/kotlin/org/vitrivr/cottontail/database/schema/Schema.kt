package org.vitrivr.cottontail.database.schema

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.mapdb.*

import org.vitrivr.cottontail.database.catalogue.Catalogue
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.column.ColumnDriver
import org.vitrivr.cottontail.database.column.mapdb.MapDBColumn
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityHeader
import org.vitrivr.cottontail.database.general.AbstractTx
import org.vitrivr.cottontail.database.general.DBO
import org.vitrivr.cottontail.database.general.TxStatus
import org.vitrivr.cottontail.database.locking.LockMode
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.utilities.extensions.read

import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.util.*
import java.util.concurrent.locks.StampedLock
import java.util.stream.Collectors

/**
 * Represents an schema in the Cottontail DB data model. A [Schema] is a collection of [Entity]
 * objects that belong together (e.g., because they belong to the same application). Every [Schema]
 * can be seen as a dedicated database and different [Schema]s in Cottontail can reside in
 * different locations.
 *
 * Calling the default constructor for [Schema] opens that [Schema]. It can only be opened once due
 * to file locks and it will remain open until the [Schema.close()] method is called.
 *
 * @see Entity
 * @see MapDBColumn
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class Schema(override val path: Path, override val parent: Catalogue) : DBO {
    /**
     * Companion object with different constants.
     */
    companion object {
        /** Filename for the [Entity] catalogue.  */
        const val SCHEMA_HEADER_FIELD = "cdb_entity_header"

        /** Filename for the [Schema] catalogue.  */
        const val FILE_CATALOGUE = "index.db"
    }

    /** Internal reference to the [Store] underpinning this [MapDBColumn]. */
    private val store: DB = try {
        this.parent.config.mapdb.db(this.path.resolve(FILE_CATALOGUE))
    } catch (e: DBException) {
        throw DatabaseException("Failed to open schema at '$path': ${e.message}'")
    }

    /** The [SchemaHeader] of this [Schema]. */
    private val headerField =
        this.store.atomicVar(SCHEMA_HEADER_FIELD, SchemaHeader.Serializer).createOrOpen()

    /** A lock used to mediate access the closed state of this [Schema]. */
    private val closeLock = StampedLock()

    /** A map of loaded [Entity] references. */
    private val registry: MutableMap<Name.EntityName, Entity> =
        Collections.synchronizedMap(Object2ObjectOpenHashMap())

    /** The [Name.SchemaName] of this [Schema]. */
    override val name: Name.SchemaName = Name.SchemaName(this.headerField.get().name)

    /** Flag indicating whether or not this [Schema] has been closed. */
    @Volatile
    override var closed: Boolean = false
        private set

    init {
        /* Initialize all entities. */
        this.headerField.get().entities.map {
            this.registry[this.name.entity(it.name)] = Entity(it.path, this)
        }
    }

    /**
     * Creates and returns a new [Schema.Tx] for the given [TransactionContext].
     *
     * @param context The [TransactionContext] to create the [Schema.Tx] for.
     * @return New [Schema.Tx]
     */
    override fun newTx(context: TransactionContext) = this.Tx(context)

    /**
     * Closes this [Schema] and all the [Entity] objects that are contained within.
     *
     * Since locks to [DBO] or [Transaction][org.vitrivr.cottontail.database.general.Tx]
     * objects may be held by other threads, it can take a
     * while for this method to complete.
     */
    override fun close() = this.closeLock.read {
        if (!this.closed) {
            this.registry.entries.removeIf {
                it.value.close()
                true
            }
            this.store.close()
            this.closed = true
        }
    }

    /**
     * A [Tx] that affects this [Schema].
     *
     * @author Ralph Gasser
     * @version 1.0.0
     */
    inner class Tx(context: TransactionContext) : AbstractTx(context), SchemaTx {

        /** Obtains a global (non-exclusive) read-lock on [Schema]. Prevents enclosing [Schema] from being closed. */
        private val closeStamp = this@Schema.closeLock.readLock()

        /** Reference to the surrounding [Schema]. */
        override val dbo: DBO
            get() = this@Schema

        /** Actions that should be executed after committing this [Tx]. */
        private val postCommitAction = mutableListOf<Runnable>()

        /** Actions that should be executed after rolling back this [Tx]. */
        private val postRollbackAction = mutableListOf<Runnable>()

        /**
         * Returns a list of [Entity] held by this [Schema].
         *
         * @return [List] of all [Name.EntityName].
         */
        override fun listEntities(): List<Name.EntityName> = this.withReadLock {
            return this@Schema.headerField.get().entities.map { this@Schema.name.entity(it.name) }
        }

        /**
         * Returns an instance of [Entity] if such an instance exists. If the [Entity] has been loaded before,
         * that [Entity] is re-used. Otherwise, the [Entity] will be loaded from disk.
         *
         * @param name Name of the [Entity] to access.
         * @return [Entity] or null.
         */
        override fun entityForName(name: Name.EntityName): Entity = this.withReadLock {
            return this@Schema.registry[name]
                ?: throw DatabaseException.EntityDoesNotExistException(name)
        }

        /**
         * Creates a new [Entity] in this [Schema].
         *
         * @param name The name of the [Entity] that should be created.
         * @param columns The [ColumnDef] of the columns the new [Entity] should have
         */
        override fun createEntity(name: Name.EntityName, vararg columns: ColumnDef<*>): Entity = this.withWriteLock {
            if (columns.map { it.name }.distinct().size != columns.size) throw DatabaseException.DuplicateColumnException(name, columns.map { it.name })
            if (this.listEntities().contains(name)) throw DatabaseException.EntityAlreadyExistsException(name)

            try {
                /* Create empty folder for entity. */
                val data = this@Schema.path.resolve("entity_${name.simple}")
                if (!Files.exists(data)) {
                    Files.createDirectories(data)
                } else {
                    throw DatabaseException("Failed to create entity '$name'. Data directory '$data' seems to be occupied.")
                }

                /* ON ROLLBACK: Delete unused entity folder. */
                this.postRollbackAction.add {
                    val pathsToDelete = Files.walk(data).sorted(Comparator.reverseOrder())
                        .collect(Collectors.toList())
                    pathsToDelete.forEach { Files.delete(it) }
                }

                /* Generate the entity and initialize the new entities header. */
                val store = this@Schema.parent.config.mapdb.db(data.resolve(Entity.CATALOGUE_FILE))
                val columnsRefs = columns.map {
                    val path = data.resolve("col_${it.name.simple}.db")
                    MapDBColumn.initialize(path, it, this@Schema.parent.config.mapdb)
                    EntityHeader.ColumnRef(it.name.simple, ColumnDriver.MAPDB, path)
                }
                val entityHeader = EntityHeader(name = name.simple, columns = columnsRefs)
                store.atomicVar(Entity.ENTITY_HEADER_FIELD, EntityHeader.Serializer).create()
                    .set(entityHeader)
                store.commit()
                store.close()

                /* Update this schema's header. */
                val oldHeader = this@Schema.headerField.get()
                val newHeader = oldHeader.copy(modified = System.currentTimeMillis())
                newHeader.addEntityRef(SchemaHeader.EntityRef(name.simple, data))
                this@Schema.headerField.compareAndSet(oldHeader, newHeader)

                /* ON COMMIT: Make entity available. */
                val entity = Entity(data, this@Schema)
                this.postCommitAction.add {
                    this@Schema.registry[name] = entity
                }

                /* ON ROLLBACK: Close entity. */
                this.postRollbackAction.add(0) {
                    entity.close()
                }

                return entity
            } catch (e: DBException) {
                this.status = TxStatus.ERROR
                throw DatabaseException("Failed to create entity '$name' due to error in the underlying data store: {${e.message}")
            } catch (e: IOException) {
                this.status = TxStatus.ERROR
                throw DatabaseException("Failed to create entity '$name' due to an IO exception: {${e.message}")
            }
        }

        /**
         * Drops an [Entity] from this [Schema].
         *
         * @param name The name of the [Entity] that should be dropped.
         */
        override fun dropEntity(name: Name.EntityName) = this.withWriteLock {
            /* Get entity and try to obtain lock. */
            val entity =
                this@Schema.registry[name] ?: throw DatabaseException.EntityDoesNotExistException(
                    name
                )
            if (this.context.lockOn(entity) == LockMode.NO_LOCK) {
                this.context.requestLock(entity, LockMode.EXCLUSIVE)
            }

            /* Close entity and remove it from registry. */
            entity.close()
            this@Schema.registry.remove(name)

            try {
                /* Rename folder and of entity it for deletion. */
                val shadowEntity = entity.path.resolveSibling(entity.path.fileName.toString() + "~dropped")
                Files.move(entity.path, shadowEntity, StandardCopyOption.ATOMIC_MOVE)

                /* ON COMMIT: Remove schema from registry and delete files. */
                this.postCommitAction.add {
                    val pathsToDelete = Files.walk(shadowEntity).sorted(Comparator.reverseOrder()).collect(Collectors.toList())
                    pathsToDelete.forEach { Files.deleteIfExists(it) }
                    this.context.releaseLock(entity)
                }

                /* ON ROLLBACK: Re-map entity and move back files. */
                this.postRollbackAction.add {
                    Files.move(shadowEntity, entity.path, StandardCopyOption.ATOMIC_MOVE)
                    this@Schema.registry[name] = Entity(entity.path, this@Schema)
                    this.context.releaseLock(entity)
                }

                /* Update this schema's header. */
                val oldHeader = this@Schema.headerField.get()
                val newHeader = oldHeader.copy(modified = System.currentTimeMillis())
                newHeader.removeEntityRef(name.simple)
                this@Schema.headerField.compareAndSet(oldHeader, newHeader)
                Unit
            } catch (e: DBException) {
                this.status = TxStatus.ERROR
                throw DatabaseException("Entity '$name' could not be dropped, because of an error in the underlying data store: ${e.message}!")
            }
        }

        /**
         * Performs a COMMIT of all changes made through this [Schema.Tx].
         */
        override fun performCommit() {
            /* Perform commit. */
            this@Schema.store.commit()

            /* Execute post-commit actions. */
            this.postCommitAction.forEach { it.run() }
            this.postRollbackAction.clear()
            this.postCommitAction.clear()
        }

        /**
         * Performs a ROLLBACK of all changes made through this [Schema.Tx].
         */
        override fun performRollback() {
            /* Perform rollback. */
            this@Schema.store.rollback()

            /* Execute post-rollback actions. */
            this.postRollbackAction.forEach { it.run() }
            this.postRollbackAction.clear()
            this.postCommitAction.clear()
        }

        /**
         * Releases the [closeLock] on the [Schema].
         */
        override fun cleanup() {
            this@Schema.closeLock.unlockRead(this.closeStamp)
        }
    }
}



