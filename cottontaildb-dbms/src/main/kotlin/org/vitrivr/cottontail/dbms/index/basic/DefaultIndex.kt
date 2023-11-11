package org.vitrivr.cottontail.dbms.index.basic

import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue.Companion.COLUMN_METADATA_STORE_NAME
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue.Companion.INDEX_METADATA_STORE_NAME
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding
import org.vitrivr.cottontail.dbms.column.ColumnMetadata
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.events.IndexEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.Transaction
import org.vitrivr.cottontail.dbms.general.DBOVersion
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * An abstract [Index] implementation that outlines the fundamental structure. Implementations of
 * [Index]es in Cottontail DB should inherit from this class.
 *
 * @see Index
 *
 * @author Ralph Gasser
 * @version 4.0.0
 */
abstract class DefaultIndex(final override val name: Name.IndexName, final override val parent: DefaultEntity): Index {
    /** A [DefaultIndex] belongs to its [DefaultCatalogue]. */
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

    /** The [DBOVersion] of this [DefaultIndex]. */
    override val version: DBOVersion = DBOVersion.V4_0

    /**
     * Compares this [DefaultIndex] to another object.
     */
    override fun equals(other: Any?): Boolean {
        if (other !is DefaultIndex) return false
        if (other.catalogue != this.catalogue) return false
        if (other.name != this.name) return false
        return true
    }

    /**
     * Hash code for this [DefaultIndex].
     */
    override fun hashCode(): Int {
        var result = name.hashCode()
        result = 31 * result + parent.hashCode()
        return result
    }

    /**
     * A [Tx] that affects this [DefaultIndex].
     */
    abstract inner class Tx(protected val parent: DefaultEntity.Tx): IndexTx, WriteModel {
        /** Reference to the Cottontail DB [Transaction] object. */
        override val transaction: Transaction = parent.transaction

        /** The Xodus [jetbrains.exodus.env.Transaction]. */
        internal val xodusTx: jetbrains.exodus.env.Transaction = parent.xodusTx

        /** Reference to the surrounding [DefaultEntity]. */
        override val dbo: DefaultIndex
            get() = this@DefaultIndex

        /** True, if the [DefaultIndex] backing this [Tx] supports incremental updates, i.e., can be updated tuple by tuple. */
        override val supportsIncrementalUpdate: Boolean
            get() = this@DefaultIndex.supportsIncrementalUpdate

        /** True, if the [Index] backing this [Tx] supports asynchronous rebuilds. */
        override val supportsAsyncRebuild: Boolean
            get() = this@DefaultIndex.supportsAsyncRebuild

        /** True, if the [DefaultIndex] backing this [Tx] supports filtering an index-able range of the data. */
        override val supportsPartitioning: Boolean
            get() = this@DefaultIndex.supportsPartitioning

        /** The [IndexConfig] used by the [DefaultIndex] this [Tx] belongs to. */
        final override val config: IndexConfig<*>

        /** The [ColumnDef] indexed by the [DefaultIndex] this [Tx] belongs to. */
        final override val columns: Array<ColumnDef<*>>

        /** A [ReentrantLock] that synchronises access to this [Tx]'s methods. */
        protected val txLatch = ReentrantLock()

        /**
         * Flag indicating, if this [DefaultIndex] reflects all changes done to the [DefaultEntity] it belongs to.
         *
         * This object is accessed lazily, since it may change within the scope of a transactio.
         */
        final override var state: IndexState
            private set

        init {
            val indexMetadataStore = this.xodusTx.environment.openStore(INDEX_METADATA_STORE_NAME, StoreConfig.USE_EXISTING, this.xodusTx, false) ?: throw DatabaseException.DataCorruptionException("Failed to open store for index catalogue.")
            val indexEntryRaw = indexMetadataStore.get(this.xodusTx, NameBinding.Index.toEntry(this@DefaultIndex.name)) ?: throw DatabaseException.DataCorruptionException("Failed to initialize transaction for index ${this@DefaultIndex.name}: Could not read catalogue entry for index.")
            val indexEntry = IndexMetadata.fromEntry(indexEntryRaw)
            this.state = indexEntry.state

            /* Read columns and config. */
            val columnMetadataStore =  this.xodusTx.environment.openStore(COLUMN_METADATA_STORE_NAME, StoreConfig.USE_EXISTING, this.xodusTx, false) ?: throw DatabaseException.DataCorruptionException("Failed to open store for column catalogue.")
            this.columns = indexEntry.columns.map {
                val columnName = this@DefaultIndex.name.entity().column(it)
                val columnEntryRaw = columnMetadataStore.get(this.xodusTx, NameBinding.Column.toEntry(columnName))  ?: throw DatabaseException.DataCorruptionException("Failed to initialize transaction for index ${this@DefaultIndex.name} because catalogue entry for column could not be read ${it}.")
                val columnEntity = ColumnMetadata.fromEntry(columnEntryRaw)
                ColumnDef(columnName, columnEntity.type, columnEntity.nullable, columnEntity.primary, columnEntity.autoIncrement)
            }.toTypedArray()
            this.config = indexEntry.config
        }

        /**
         * Tries to process an incoming [DataEvent.Insert].
         *
         * If the [DefaultIndex] does not support incremental updates, the [DefaultIndex] will be marked as [IndexState.STALE].
         * Otherwise, the change is propagated to the [DefaultIndex].
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
         * If the [DefaultIndex] does not support incremental updates, the [DefaultIndex] will be marked as [IndexState.STALE].
         * Otherwise, the change is propagated to the [DefaultIndex].
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
         * If the [DefaultIndex] does not support incremental updates, the [DefaultIndex] will be marked as [IndexState.STALE].
         * Otherwise, the change is propagated to the [DefaultIndex].
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
         * Convenience method to update [IndexState] for this [DefaultIndex].
         *
         * @param state The new [IndexState].
         */
        private fun updateState(state: IndexState) {
            if (state != this.state) {
                val name = NameBinding.Index.toEntry(this@DefaultIndex.name)
                val store = this.xodusTx.environment.openStore(INDEX_METADATA_STORE_NAME, StoreConfig.USE_EXISTING, this.xodusTx, false) ?: throw DatabaseException.DataCorruptionException("Failed to open store for index catalogue.")
                val entry = IndexMetadata(this@DefaultIndex.type, state, this.columns.map { it.name.columnName }, this.config)
                if (store.put(this.xodusTx, name, IndexMetadata.toEntry(entry))) {
                    this.state = state
                    this.transaction.signalEvent(IndexEvent.State(this@DefaultIndex.name, this@DefaultIndex.type, state))
                }
            }
        }
    }
}
