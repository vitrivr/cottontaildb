package org.vitrivr.cottontail.dbms.index

import jetbrains.exodus.env.Store
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.entries.ColumnCatalogueEntry
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexCatalogueEntry
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.exceptions.TxException
import org.vitrivr.cottontail.dbms.execution.TransactionContext
import org.vitrivr.cottontail.dbms.general.AbstractTx
import org.vitrivr.cottontail.dbms.general.DBOVersion
import org.vitrivr.cottontail.dbms.queries.sort.SortOrder
import kotlin.concurrent.withLock

/**
 * An abstract [Index] implementation that outlines the fundamental structure. Implementations of
 * [Index]es in Cottontail DB should inherit from this class.
 *
 * @see Index
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
abstract class AbstractIndex(final override val name: Name.IndexName, final override val parent: DefaultEntity) : Index {

    /** A [AbstractIndex] belongs to its [DefaultCatalogue]. */
    final override val catalogue: DefaultCatalogue = this.parent.catalogue

    /** The [ColumnDef] that are covered (i.e. indexed) by this [AbstractIndex]. */
    final override val columns: Array<ColumnDef<*>>
        get() = this.catalogue.environment.computeInTransaction { tx ->
            IndexCatalogueEntry.read(this.name, this.catalogue, tx)?.columns?.map {
                ColumnCatalogueEntry.read(it, this.catalogue, tx)?.toColumnDef() ?: throw DatabaseException.DataCorruptionException("Failed to obtain columns for index ${this.name}: Could not read catalogue entry for column ${it}.")
            }?.toTypedArray() ?: throw DatabaseException.DataCorruptionException("Failed to obtain columns for index ${this.name}: Could not read catalogue entry for index.")
        }

    /**
     * Flag indicating, whether this [AbstractIndex] reflects all changes done to the [DefaultEntity] it belongs to.
     *
     * This is a snapshot and may change immediately!
     */
    final override val state: IndexState
        get() = this.catalogue.environment.computeInTransaction { tx ->
            IndexCatalogueEntry.read(this.name, this.catalogue, tx)?.state
                ?: throw DatabaseException.DataCorruptionException("Failed to obtain state for index ${this.name}: Could not read catalogue entry for index.")
        }

    /**
     * Number of entries in this [AbstractIndex].
     *
     * This is a snapshot and may change immediately.
     */
    final override val count: Long
        get() = this.catalogue.environment.computeInTransaction { tx ->
            this.catalogue.environment.openStore(this.name.storeName(), this.type.storeConfig, tx).count(tx)
        }

    /** The order in which results of this [Index] appear. Defaults to an empty array, which indicates no particular order. */
    override val order: Array<Pair<ColumnDef<*>, SortOrder>> = emptyArray()

    /** The [DBOVersion] of this [AbstractIndex]. */
    override val version: DBOVersion = DBOVersion.V3_0

    /** Flag indicating if this [AbstractIndex] has been closed. */
    override val closed: Boolean
        get() = this.parent.closed

    /**
     * A [Tx] that affects this [AbstractIndex].
     */
    protected abstract inner class Tx(context: TransactionContext) : AbstractTx(context), IndexTx {

        /** Internal data [Store] reference. */
        protected var dataStore: Store = this@AbstractIndex.catalogue.environment.openStore(this@AbstractIndex.name.storeName(), this@AbstractIndex.type.storeConfig, this.context.xodusTx, false)
            ?: throw DatabaseException.DataCorruptionException("Data store for index ${this@AbstractIndex.name} (${this@AbstractIndex.type}) is missing.")

        /** Reference to the [AbstractIndex] */
        final override val dbo: AbstractIndex
            get() = this@AbstractIndex

        /** The order in which results of this [IndexTx] appear. Empty array that there is no particular order. */
        override val order: Array<Pair<ColumnDef<*>, SortOrder>>
            get() = this@AbstractIndex.order

        /** The [IndexType] of the [AbstractIndex] that underpins this [IndexTx]. */
        override val type: IndexType
            get() = this@AbstractIndex.type

        /** Flag indicating, if this [AbstractIndex] reflects all changes done to the [DefaultEntity]it belongs to. */
        override var state: IndexState = IndexCatalogueEntry.read(this@AbstractIndex.name, this@AbstractIndex.catalogue, this.context.xodusTx)?.state
            ?: throw DatabaseException.DataCorruptionException("Failed to obtain state for index ${this@AbstractIndex.name}: Could not read catalogue entry for index.")

        /** The [ColumnDef] indexed by the [AbstractIndex] this [Tx] belongs to. */
        override val columns: Array<ColumnDef<*>> = IndexCatalogueEntry.read(this@AbstractIndex.name, this@AbstractIndex.catalogue, this.context.xodusTx)?.columns?.map {
                ColumnCatalogueEntry.read(it, this@AbstractIndex.catalogue, this.context.xodusTx)?.toColumnDef() ?: throw DatabaseException.DataCorruptionException("Failed to obtain columns for index ${this@AbstractIndex.name}: Could not read catalogue entry for column ${it}.")
            }?.toTypedArray() ?: throw DatabaseException.DataCorruptionException("Failed to obtain columns for index ${this@AbstractIndex.name}: Could not read catalogue entry for index.")

        /**
         * Obtains a global (non-exclusive) read-lock on [DefaultCatalogue].
         *
         * Prevents [DefaultCatalogue] from being closed while transaction is ongoing.
         */
        private val closeStamp = this.dbo.catalogue.closeLock.readLock()

        init {
            /** Checks if DBO is still open. */
            if (this.dbo.closed) {
                this.dbo.catalogue.closeLock.unlockRead(this.closeStamp)
                throw TxException.TxDBOClosedException(this.context.txId, this.dbo)
            }
        }

        /**
         * Returns the number of entries in this [AbstractIndex].
         *
         * @return Number of entries in this [AbstractIndex]
         */
        override fun count(): Long  = this.txLatch.withLock {
            this.dataStore.count(this.context.xodusTx)
        }

        /**
         * Clears the [AbstractTx] underlying this [Tx] and removes all entries it contains.
         */
        override fun clear() = this.txLatch.withLock {
            /* Truncate and replace store.*/
            this@AbstractIndex.catalogue.environment.truncateStore(this@AbstractIndex.name.storeName(), this.context.xodusTx)
            this.dataStore = this@AbstractIndex.catalogue.environment.openStore(this@AbstractIndex.name.storeName(),this@AbstractIndex.type.storeConfig, this.context.xodusTx, false)
                ?: throw DatabaseException.DataCorruptionException("Data store for index ${this@AbstractIndex.name} (${this@AbstractIndex.type}) is missing.")

            /* Update catalogue entry for index. */
            this.updateState(IndexState.STALE)
        }

        /**
         * Convenience method to update [IndexState] for this [AbstractHDIndex].
         *
         * @param state The new [IndexState].
         */
        protected fun updateState(state: IndexState) {
            val entry = IndexCatalogueEntry.read(this@AbstractIndex.name, this@AbstractIndex.catalogue, this.context.xodusTx) ?:
            throw DatabaseException.DataCorruptionException("Failed to update state for index ${this@AbstractIndex.name}: Could not read catalogue entry for index.")
            if (!IndexCatalogueEntry.write(entry.copy(state = state), this@AbstractIndex.catalogue, this.context.xodusTx)) {
                throw DatabaseException.DataCorruptionException("Failed to update state for index ${this@AbstractIndex.name}: Could not update catalogue entry for index.")
            }
            this.state = state
        }

        /**
         * Checks if this [IndexTx] can process the provided [Predicate].
         *
         * @param predicate [Predicate] to check.
         * @return True if [Predicate] can be processed, false otherwise.
         */
        override fun canProcess(predicate: Predicate): Boolean = this@AbstractIndex.canProcess(predicate)

        /**
         * Called when a transaction finalizes. Releases the lock held on the [DefaultCatalogue].
         */
        override fun cleanup() {
            this.dbo.catalogue.closeLock.unlockRead(this.closeStamp)
        }
    }
}
