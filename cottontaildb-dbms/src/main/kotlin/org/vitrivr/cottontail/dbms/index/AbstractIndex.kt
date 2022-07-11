package org.vitrivr.cottontail.dbms.index

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.entries.ColumnCatalogueEntry
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexCatalogueEntry
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.events.IndexEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.exceptions.TransactionException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.general.AbstractTx
import org.vitrivr.cottontail.dbms.general.DBOVersion
import org.vitrivr.cottontail.dbms.index.basics.avc.AuxiliaryValueCollection
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

    /** True, if the [Index] supports incremental updates, i.e., can be updated tuple by tuple. Determined by its [IndexDescriptor]. */
    final override val supportsIncrementalUpdate: Boolean
        get() = this.type.descriptor.supportsIncrementalUpdate

    /** True, if the [Index] backing this [IndexTx] supports asynchronous rebuilds. Determined by its [IndexDescriptor] */
    final override val supportsAsyncRebuild: Boolean
        get() = this.type.descriptor.supportsAsyncRebuild

    /** True, if the [Index] supports filtering an index-able range of the data. Determined by its [IndexDescriptor] */
    final override val supportsPartitioning: Boolean
        get() = this.type.descriptor.supportsPartitioning

    /** The [DBOVersion] of this [AbstractIndex]. */
    override val version: DBOVersion = DBOVersion.V3_0

    /** Flag indicating if this [AbstractIndex] has been closed. */
    override val closed: Boolean
        get() = this.parent.closed

    /**
     * A [Tx] that affects this [AbstractIndex].
     */
    protected abstract inner class Tx(context: TransactionContext) : AbstractTx(context), IndexTx, WriteModel {

        /** Reference to the [AbstractIndex] */
        final override val dbo: AbstractIndex
            get() = this@AbstractIndex

        /** True, if the [AbstractIndex] backing this [Tx] supports incremental updates, i.e., can be updated tuple by tuple. */
        override val supportsIncrementalUpdate: Boolean
            get() = this@AbstractIndex.supportsIncrementalUpdate

        /** True, if the [Index] backing this [Tx] supports asynchronous rebuilds. */
        override val supportsAsyncRebuild: Boolean
            get() = this@AbstractIndex.supportsAsyncRebuild

        /** True, if the [AbstractIndex] backing this [Tx] supports filtering an index-able range of the data. */
        override val supportsPartitioning: Boolean
            get() = this@AbstractIndex.supportsPartitioning

        /** The [ColumnDef] indexed by the [AbstractIndex] this [Tx] belongs to. */
        override val columns: Array<ColumnDef<*>> = IndexCatalogueEntry.read(this@AbstractIndex.name, this@AbstractIndex.catalogue, this.context.xodusTx)?.columns?.map {
                ColumnCatalogueEntry.read(it, this@AbstractIndex.catalogue, this.context.xodusTx)?.toColumnDef() ?: throw DatabaseException.DataCorruptionException("Failed to obtain columns for index ${this@AbstractIndex.name} because catalogue entry for column could not be read ${it}.")
            }?.toTypedArray() ?: throw DatabaseException.DataCorruptionException("Failed to obtain columns for index ${this@AbstractIndex.name}: Could not read catalogue entry for index.")

        /**
         * Flag indicating, if this [AbstractIndex] reflects all changes done to the [DefaultEntity] it belongs to.
         *
         * This object is accessed lazily, since it may change within the scope of a transactio.
         */
        override val state: IndexState
            get() = IndexCatalogueEntry.read(this@AbstractIndex.name, this@AbstractIndex.catalogue, this.context.xodusTx)?.state ?: throw DatabaseException.DataCorruptionException("Failed to obtain state for index ${this@AbstractIndex.name}: Could not read catalogue entry for index.")

        /**
         * Accessor for the [IndexConfig].
         *
         * This object is accessed lazily, since it may change within the scope of a transaction.
         */
        override val config: IndexConfig<*>
            get() {
                val entry = IndexCatalogueEntry.read(this@AbstractIndex.name, this@AbstractIndex.catalogue, this.context.xodusTx) ?: throw DatabaseException.DataCorruptionException("Failed to read catalogue entry for index ${this@AbstractIndex.name}.")
                return entry.config
            }

        /** The number of INSERT operations since last rebuilding the index. */
        protected var numberOfInserts = 0L

        /** The number of UPDATE operations since last rebuilding the index. */
        protected var numberOfUpdates = 0L

        /** The number of DELETE operations since last rebuilding the index. */
        protected var numberOfDeletes = 0L

        /**
         * Obtains a global (non-exclusive) read-lock on [DefaultCatalogue].
         *
         * Prevents [DefaultCatalogue] from being closed while transaction is ongoing.
         */
        private val closeStamp: Long

        init {
            /** Checks if DBO is still open. */
            if (this.dbo.closed) throw TransactionException.DBOClosed(this.context.txId, this.dbo)
            this.closeStamp = this.dbo.catalogue.closeLock.readLock()
        }

        /**
         * Tries to process an incoming [DataEvent.Insert].
         *
         * If the [AbstractIndex] does not support incremental updates, the [AbstractIndex] will be marked as [IndexState.STALE].
         * Otherwise, the change is either propagated to the [AbstractIndex] or to the [AuxiliaryValueCollection] this marking the
         * [AbstractIndex] as [IndexState.DIRTY].
         *
         * @param event [DataEvent.Insert] that should be processed,
         */
        final override fun insert(event: DataEvent.Insert) = this.txLatch.withLock {
            /* If write-model does not allow propagation, mark index as STALE. */
            if (this.tryApply(event)) {
                this.numberOfInserts += 1
            } else {
                this.updateState(IndexState.STALE)
            }
        }

        /**
         * Tries to process an incoming [DataEvent.Update].
         *
         * If the [AbstractIndex] does not support incremental updates, the [AbstractIndex] will be marked as [IndexState.STALE].
         * Otherwise, the change is either propagated to the [AbstractIndex] or to the [AuxiliaryValueCollection] this marking the
         * [AbstractIndex] as [IndexState.DIRTY].
         *
         * @param event [DataEvent.Update]
         */
        final override fun update(event: DataEvent.Update) = this.txLatch.withLock {
            /* If write-model does not allow propagation, mark index as STALE. */
            if (this.tryApply(event)) {
                this.numberOfUpdates += 1
            } else {
                this.updateState(IndexState.STALE)
            }
        }

        /**
         * Tries to process an incoming [DataEvent.Delete].
         *
         * If the [AbstractIndex] does not support incremental updates, the [AbstractIndex] will be marked as [IndexState.STALE].
         * Otherwise, the change is either propagated to the [AbstractIndex] or to the [AuxiliaryValueCollection] this marking the
         * [AbstractIndex] as [IndexState.DIRTY].
         *
         * @param event [DataEvent.Delete] that should be processed.
         */
        final override fun delete(event: DataEvent.Delete) = this.txLatch.withLock {
            /* If write-model does not allow propagation, mark index as dirty. */
            if (this.tryApply(event)) {
                this.numberOfDeletes += 1
            } else {
                this.updateState(IndexState.STALE)
            }
        }

        /**
         * Convenience method to update [IndexState] for this [AbstractIndex].
         *
         * @param state The new [IndexState].
         */
        internal fun updateState(state: IndexState, config: IndexConfig<*>? = null) {
            /* Obtain old entry and compare state. */
            val oldEntry = IndexCatalogueEntry.read(this@AbstractIndex.name, this@AbstractIndex.catalogue, this.context.xodusTx) ?: throw DatabaseException.DataCorruptionException("Failed to update state for index ${this@AbstractIndex.name}: Could not read catalogue entry for index.")
            if (oldEntry.state == state) return

            /* Copy entry... */
            val newEntry = if (config != null) {
                oldEntry.copy(state = state, config = config)
            } else {
                oldEntry.copy(state = state)
            }

            /* ... and write it to catalogue. */
            IndexCatalogueEntry.write(newEntry, this@AbstractIndex.catalogue, this.context.xodusTx)

            /* Signal event to transaction context. */
            this.context.signalEvent(IndexEvent.State(this@AbstractIndex.name, this@AbstractIndex.type, state))
        }

        /**
         * Called when a transaction finalizes. Releases the lock held on the [DefaultCatalogue].
         */
        override fun cleanup() {
            this.dbo.catalogue.closeLock.unlockRead(this.closeStamp)
        }
    }
}
