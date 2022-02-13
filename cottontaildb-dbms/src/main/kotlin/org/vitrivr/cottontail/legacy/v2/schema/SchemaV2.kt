package org.vitrivr.cottontail.legacy.v2.schema

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.mapdb.DB
import org.mapdb.DBException
import org.mapdb.Store
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.exceptions.TxException
import org.vitrivr.cottontail.dbms.execution.TransactionContext
import org.vitrivr.cottontail.dbms.general.AbstractTx
import org.vitrivr.cottontail.dbms.general.DBO
import org.vitrivr.cottontail.dbms.general.DBOVersion
import org.vitrivr.cottontail.dbms.schema.Schema
import org.vitrivr.cottontail.dbms.schema.SchemaTx
import org.vitrivr.cottontail.legacy.v2.catalogue.CatalogueV2
import org.vitrivr.cottontail.legacy.v2.entity.EntityV2
import org.vitrivr.cottontail.utilities.extensions.write
import java.nio.file.Path
import java.util.*
import java.util.concurrent.locks.StampedLock

/**
 * Default [Schema] implementation in Cottontail DB based on Map DB.
 *
 * @see Schema
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class SchemaV2(val path: Path, override val parent: CatalogueV2) : Schema, AutoCloseable {
    /** Companion object with different constants. */
    companion object {
        /** Filename for the [EntityV2] catalogue.  */
        private const val SCHEMA_HEADER_FIELD = "cdb_entity_header"

        /** Filename for the [SchemaV2] catalogue.  */
        private const val FILE_CATALOGUE = "index.db"
    }

    /** Internal reference to the [Store] underpinning this [SchemaV2]. */
    private val store: DB = try {
        this.parent.config.mapdb.db(this.path.resolve(FILE_CATALOGUE))
    } catch (e: DBException) {
        throw DatabaseException("Failed to open schema at '$path': ${e.message}'")
    }

    /** The [SchemaHeader] of this [SchemaV2]. */
    private val headerField = this.store.atomicVar(SCHEMA_HEADER_FIELD, SchemaHeader.Serializer).createOrOpen()

    /** A lock used to mediate access the closed state of this [SchemaV2]. */
    private val closeLock = StampedLock()

    /** A map of loaded [EntityV2] references. */
    private val registry: MutableMap<Name.EntityName, EntityV2> = Collections.synchronizedMap(Object2ObjectOpenHashMap())

    /** The [Name.SchemaName] of this [SchemaV2]. */
    override val name: Name.SchemaName = Name.SchemaName(this.headerField.get().name)

    /** The [Catalogue] this [SchemaV2] belongs to. */
    override val catalogue: Catalogue
        get() = this.parent.catalogue

    /** The [DBOVersion] of this [SchemaV2]. */
    override val version: DBOVersion
        get() = DBOVersion.V2_0

    /** Flag indicating whether this [SchemaV2] has been closed. */
    override val closed: Boolean
        get() = this.store.isClosed()

    init {
        /* Initialize all entities. */
        this.headerField.get().entities.map {
            val path = this.path.resolve("entity_${it.name}")
            this.registry[this.name.entity(it.name)] = EntityV2(path, this)
        }
    }

    /**
     * Creates and returns a new [SchemaV2.Tx] for the given [TransactionContext].
     *
     * @param context The [TransactionContext] to create the [SchemaV2.Tx] for.
     * @return New [SchemaV2.Tx]
     */
    override fun newTx(context: TransactionContext) = this.Tx(context)

    /**
     * Closes this [SchemaV2] and all the [EntityV2] objects that are contained within.
     *
     * Since locks to [DBO] or [Transaction][org.vitrivr.cottontail.dbms.general.Tx]
     * objects may be held by other threads, it can take a
     * while for this method to complete.
     */
    override fun close() = this.closeLock.write {
        if (!this.closed) {
            this.store.close()
            this.registry.entries.removeIf {
                it.value.close()
                true
            }
        }
    }

    /**
     * A [Tx] that affects this [SchemaV2].
     *
     * @author Ralph Gasser
     * @version 2.0.0
     */
    inner class Tx(context: TransactionContext) : AbstractTx(context), SchemaTx {

        /** Obtains a global (non-exclusive) read-lock on [SchemaV2]. Prevents enclosing [SchemaV2] from being closed. */
        private val closeStamp = this@SchemaV2.closeLock.readLock()

        /** Reference to the surrounding [SchemaV2]. */
        override val dbo: DBO
            get() = this@SchemaV2

        /** Checks if DBO is still open. */
        init {
            if (this@SchemaV2.closed) {
                this@SchemaV2.closeLock.unlockRead(this.closeStamp)
                throw TxException.TxDBOClosedException(this.context.txId, this@SchemaV2)
            }
        }

        /**
         * Returns a list of [EntityV2] held by this [SchemaV2].
         *
         * @return [List] of all [Name.EntityName].
         */
        override fun listEntities(): List<Name.EntityName> {
           return this@SchemaV2.registry.values.map { it.name }.toList()
        }

        /**
         * Returns an instance of [EntityV2] if such an instance exists. If the [EntityV2] has been loaded before,
         * that [EntityV2] is re-used. Otherwise, the [EntityV2] will be loaded from disk.
         *
         * @param name Name of the [EntityV2] to access.
         * @return [EntityV2] or null.
         */
        override fun entityForName(name: Name.EntityName): Entity {
            return this@SchemaV2.registry[name] ?: throw DatabaseException.EntityDoesNotExistException(name)
        }

        override fun createEntity(name: Name.EntityName, vararg columns: ColumnDef<*>): EntityV2 {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }

        override fun dropEntity(name: Name.EntityName) {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }

        /**
         * Releases the [closeLock] on the [SchemaV2].
         */
        override fun cleanup() {
            this@SchemaV2.closeLock.unlockRead(this.closeStamp)
        }
    }
}