package org.vitrivr.cottontail.database.index

import org.mapdb.DB
import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.database.column.Column
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.AbstractTx
import org.vitrivr.cottontail.database.general.DBO
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.database.schema.Schema
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.exceptions.DatabaseException
import org.vitrivr.cottontail.model.exceptions.TxException
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import java.util.concurrent.locks.StampedLock

/**
 * Represents an [Index] structure in the Cottontail DB data model. An [Index] belongs to an [Entity]
 * and can be used to index one to many [Column]s. Usually, [Index]es allow for faster data access.
 *
 * @see Schema
 * @see Column
 * @see Entity.Tx
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
abstract class Index(final override val path: Path, final override val parent: Entity) : DBO {
    /**
     * Companion object of the [Entity]
     */
    companion object {

        /** Field name for the [IndexHeader] entry.  */
        const val INDEX_HEADER_FIELD = "cdb_index_header"

        /** Field name for the index configuration entries.  */
        const val INDEX_CONFIG_FIELD = "cdb_index_config"

        /** Field name for the [Index]es 'dirty' entry.  */
        const val INDEX_DIRTY_FIELD = "cdb_index_dirty"

        /**
         * Initializes a new Cottontail DB [Index].
         *
         * @param path [Path] to the index.
         * @param name The [Name] of the index.
         * @param type The [IndexType] of the [IndexHeader]
         * @param columns The [ColumnDef] indexed by the [IndexHeader]
         * @param config The Cottontail DB  configuration
         */
        fun initialize(
            path: Path,
            name: Name.IndexName,
            type: IndexType,
            columns: Array<ColumnDef<*>>,
            config: Config
        ) {
            if (Files.exists(path)) throw DatabaseException.InvalidFileException("Could not initialize index. A file already exists under $path.")
            val db: DB = config.mapdb.db(path)
            val header = db.atomicVar(INDEX_HEADER_FIELD, IndexHeader.Serializer).create()
            header.set(IndexHeader(name.simple, type, columns))
            db.commit()
            db.close()
        }
    }

    /** An internal lock that is used to synchronize structural changes to an [Index] (e.g. closing or deleting) with running [Index.Tx]. */
    protected val closeLock = StampedLock()

    /** The internal [DB] reference for this [Index]. */
    protected val db: DB = this.parent.parent.parent.config.mapdb.db(this.path)

    /** The [IndexHeader] for this [Index]. */
    protected val headerField =
        this.db.atomicVar(INDEX_HEADER_FIELD, IndexHeader.Serializer).createOrOpen()

    /** Internal storage variable for the dirty flag. */
    protected val dirtyField = this.db.atomicBoolean(INDEX_DIRTY_FIELD).createOrOpen()

    /** The [Name.IndexName] of this [Index]. */
    override val name: Name.IndexName = this.parent.name.index(this.headerField.get().name)

    /** The [ColumnDef] that are covered (i.e. indexed) by this [Index]. */
    val columns: Array<ColumnDef<*>> = this.headerField.get().columns

    /** The [ColumnDef] that are produced by this [Index]. They often differ from the indexed columns. */
    abstract val produces: Array<ColumnDef<*>>

    /** The type of [Index]. */
    abstract val type: IndexType

    /** True, if the [Index] supports incremental updates, and false otherwise. */
    abstract val supportsIncrementalUpdate: Boolean

    /** True, if the [Index] supports querying filtering an indexable range of the data. */
    abstract val supportsPartitioning: Boolean

    /** Flag indicating, if this [Index] reflects all changes done to the [Entity]it belongs to. */
    val dirty: Boolean
        get() = this.dirtyField.get()

    /**
     * Checks if this [Index] can process the provided [Predicate] and returns true if so and false otherwise.
     *
     * @param predicate [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    abstract fun canProcess(predicate: Predicate): Boolean

    /**
     * Calculates the cost estimate if this [Index] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    abstract fun cost(predicate: Predicate): Cost

    /**
     * Handles finalization, in case the Garbage Collector reaps a cached [Index].
     */
    @Synchronized
    protected fun finalize() {
        this.close()
    }

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [Index].
     *
     * @param context If the [TransactionContext] that requested the [IndexTx].
     */
    abstract override fun newTx(context: TransactionContext): IndexTx

    /**
     * A [Tx] that affects this [Index].
     */
    protected abstract inner class Tx(context: TransactionContext) : AbstractTx(context), IndexTx {

        init {
            if (this@Index.closed) {
                throw TxException.TxDBOClosedException(this.context.txId)
            }
        }

        /** Obtains a global (non-exclusive) read-lock on [Index]. Prevents enclosing [Index] from being closed. */
        private val closeStamp = this@Index.closeLock.readLock()

        /** Reference to the [Index] */
        override val dbo: Index
            get() = this@Index

        /** The simple [Name]s of the [Index] that underpins this [IndexTx] */
        override val name: Name
            get() = this@Index.name

        /** The [ColumnDef]s covered by the [Index] that underpins this [IndexTx]. */
        override val columns: Array<ColumnDef<*>>
            get() = this@Index.columns

        /** The [ColumnDef]s returned by the [Index] that underpins this [IndexTx]. */
        override val produces: Array<ColumnDef<*>>
            get() = this@Index.produces

        /** The [IndexType] of the [Index] that underpins this [IndexTx]. */
        override val type: IndexType
            get() = this@Index.type

        /**
         * Checks if this [IndexTx] can process the provided [Predicate].
         *
         * @param predicate [Predicate] to check.
         * @return True if [Predicate] can be processed, false otherwise.
         */
        override fun canProcess(predicate: Predicate): Boolean = this@Index.canProcess(predicate)

        /**
         * Releases the [closeLock] in the [Index].
         */
        override fun cleanup() {
            this@Index.closeLock.unlock(this.closeStamp)
        }
    }
}
