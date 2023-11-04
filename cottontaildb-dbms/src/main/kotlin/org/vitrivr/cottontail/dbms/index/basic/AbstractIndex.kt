package org.vitrivr.cottontail.dbms.index.basic

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding
import org.vitrivr.cottontail.dbms.column.ColumnMetadata
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.events.IndexEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.general.AbstractTx
import org.vitrivr.cottontail.dbms.general.DBOVersion
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import kotlin.concurrent.withLock

/**
 * An abstract [Index] implementation that outlines the fundamental structure. Implementations of
 * [Index]es in Cottontail DB should inherit from this class.
 *
 * @see Index
 *
 * @author Ralph Gasser
 * @version 3.1.0
 */
abstract class AbstractIndex(final override val name: Name.IndexName, final override val parent: DefaultEntity): Index {

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

    override fun equals(other: Any?): Boolean {
        if (other !is AbstractIndex) return false
        if (other.catalogue != this.catalogue) return false
        if (other.name != this.name) return false
        return true
    }
    override fun hashCode(): Int {
        var result = name.hashCode()
        result = 31 * result + parent.hashCode()
        return result
    }

    /**
     * A [Tx] that affects this [AbstractIndex].
     */
    abstract inner class Tx(context: QueryContext) : AbstractTx(context), IndexTx, WriteModel {

        init {
            /* Cache this Tx for future use. */
            context.txn.cacheTx(this)
        }

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

        /** The [IndexConfig] used by the [AbstractIndex] this [Tx] belongs to. */
        final override val config: IndexConfig<*>

        /** The [ColumnDef] indexed by the [AbstractIndex] this [Tx] belongs to. */
        final override val columns: Array<ColumnDef<*>>

        /**
         * Flag indicating, if this [AbstractIndex] reflects all changes done to the [DefaultEntity] it belongs to.
         *
         * This object is accessed lazily, since it may change within the scope of a transactio.
         */
        final override var state: IndexState
            private set

        init {
            val indexMetadataStore = IndexMetadata.store(this@AbstractIndex.catalogue, this.context.txn.xodusTx)
            val indexEntryRaw = indexMetadataStore.get(this.context.txn.xodusTx, NameBinding.Index.toEntry(this@AbstractIndex.name)) ?: throw DatabaseException.DataCorruptionException("Failed to initialize transaction for index ${this@AbstractIndex.name}: Could not read catalogue entry for index.")
            val indexEntry = IndexMetadata.fromEntry(indexEntryRaw)
            this.state = indexEntry.state

            /* Read columns and config. */
            val columnMetadataStore =  ColumnMetadata.store(this@AbstractIndex.catalogue, this.context.txn.xodusTx)
            this.columns = indexEntry.columns.map {
                val columnName = this@AbstractIndex.name.entity().column(it)
                val columnEntryRaw = columnMetadataStore.get(this.context.txn.xodusTx, NameBinding.Column.toEntry(columnName))  ?: throw DatabaseException.DataCorruptionException("Failed to initialize transaction for index ${this@AbstractIndex.name} because catalogue entry for column could not be read ${it}.")
                val columnEntity = ColumnMetadata.fromEntry(columnEntryRaw)
                ColumnDef(columnName, columnEntity.type, columnEntity.nullable, columnEntity.primary, columnEntity.autoIncrement)
            }.toTypedArray()
            this.config = indexEntry.config
        }

        /**
         * Tries to process an incoming [DataEvent.Insert].
         *
         * If the [AbstractIndex] does not support incremental updates, the [AbstractIndex] will be marked as [IndexState.STALE].
         * Otherwise, the change is propagated to the [AbstractIndex].
         *
         * @param event [DataEvent.Insert] that should be processed,
         */
        final override fun insert(event: DataEvent.Insert) = this.txLatch.withLock {
            if (this.state != IndexState.STALE) { /* Stale indexes are no longer updated; if write-model does not allow propagation, mark index as STALE. */
                if (!this.tryApply(event)) {
                    this.updateState(IndexState.STALE)
                }
            }
        }

        /**
         * Tries to process an incoming [DataEvent.Update].
         *
         * If the [AbstractIndex] does not support incremental updates, the [AbstractIndex] will be marked as [IndexState.STALE].
         * Otherwise, the change is propagated to the [AbstractIndex].
         *
         * @param event [DataEvent.Update]
         */
        final override fun update(event: DataEvent.Update) = this.txLatch.withLock {
            if (this.state != IndexState.STALE) { /* Stale indexes are no longer updated; if write-model does not allow propagation, mark index as STALE. */
                if (!this.tryApply(event)) {
                    this.updateState(IndexState.STALE)
                }
            }
        }

        /**
         * Tries to process an incoming [DataEvent.Delete].
         *
         * If the [AbstractIndex] does not support incremental updates, the [AbstractIndex] will be marked as [IndexState.STALE].
         * Otherwise, the change is propagated to the [AbstractIndex].
         *
         * @param event [DataEvent.Delete] that should be processed.
         */
        final override fun delete(event: DataEvent.Delete) = this.txLatch.withLock {
            if (this.state != IndexState.STALE) { /* Stale indexes are no longer updated; if write-model does not allow propagation, mark index as STALE. */
                if (!this.tryApply(event)) {
                    this.updateState(IndexState.STALE)
                }
            }
        }

        /**
         * Convenience method to update [IndexState] for this [AbstractIndex].
         *
         * @param state The new [IndexState].
         */
        private fun updateState(state: IndexState) {
            if (state != this.state) {
                val name = NameBinding.Index.toEntry(this@AbstractIndex.name)
                val store = IndexMetadata.store(this@AbstractIndex.catalogue, this.context.txn.xodusTx)
                val entry = IndexMetadata(this@AbstractIndex.type, state, this.columns.map { it.name.columnName }, this.config)
                if (store.put(this.context.txn.xodusTx, name, IndexMetadata.toEntry(entry))) {
                    this.state = state
                    this.context.txn.signalEvent(IndexEvent.State(this@AbstractIndex.name, this@AbstractIndex.type, state))
                }
            }
        }
    }
}
