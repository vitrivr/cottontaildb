package org.vitrivr.cottontail.dbms.index.basic.rebuilder

import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.index.basic.*
import org.vitrivr.cottontail.dbms.index.basic.IndexMetadata.Companion.storeName
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * An abstract [IndexRebuilder] implementation.
 *
 * @see [IndexRebuilder]
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
abstract class AbstractIndexRebuilder<T: Index>(final override val index: T, final override val context: QueryContext): IndexRebuilder<T> {
    companion object {
        /** [Logger] instance used by [AbstractIndexRebuilder]. */
        @JvmStatic
        protected val LOGGER: Logger = LoggerFactory.getLogger(AbstractIndexRebuilder::class.java)
    }

    /**
     * Starts the index rebuilding process for this [AbstractIndexRebuilder].
     *
     * @return True on success, false otherwise.
     */
    @Synchronized
    override fun rebuild(indexTx: IndexTx): Boolean {
        require(indexTx is AbstractIndex.Tx) { "AbstractIndexRebuilder can only be accessed with a AbstractIndex.Tx!" }

        /* Sanity check. */
        require(this.context.txn.type.exclusive) { "Failed to rebuild index ${this.index.name} (${this.index.type}); rebuild operation requires exclusive transaction."}
        LOGGER.debug("Rebuilding index {} ({}).", this.index.name, this.index.type)

        /* Clear store and update state of index (* ---> DIRTY). */
        if (!this.updateState(IndexState.DIRTY, indexTx)) {
            LOGGER.error("Rebuilding index ${this.index.name} (${this.index.type}) failed because index state could not be changed to CLEAN!")
            return false
        }

        /* Execute rebuild operation*/
        if (!this.rebuildInternal(indexTx)) {
            LOGGER.error("Rebuilding index ${this.index.name} (${this.index.type}) failed!")
            return false
        }

        /* Update state of index (DIRTY ---> CLEAN). */
        if (!this.updateState(IndexState.CLEAN, indexTx)) {
            LOGGER.error("Rebuilding index ${this.index.name} (${this.index.type}) failed because index state could not be changed to CLEAN!")
            return false
        }

        LOGGER.debug("Rebuilding index {} ({}) completed!", this.index.name, this.index.type)
        return true
    }

    /**
     * Internal implementation of the actual re-indexing procedure.
     *
     * @param indexTx The [AbstractIndex.Tx] to use.*
     * @return True on success, false otherwise.
     */
    protected abstract fun rebuildInternal(indexTx: AbstractIndex.Tx): Boolean

    /**
     * Clears and opens the data store associated with this [Index].
     *
     * @param indexTx The [AbstractIndex.Tx] to use.
     * @return [Store]
     */
    protected fun tryClearAndOpenStore(indexTx: AbstractIndex.Tx): Store? {
        val storeName = this.index.name.storeName()
        if (indexTx.xodusTx.environment.storeExists(storeName, indexTx.xodusTx)) {
            indexTx.xodusTx.environment.truncateStore(storeName, indexTx.xodusTx)
            return indexTx.xodusTx.environment.openStore(storeName, StoreConfig.USE_EXISTING, indexTx.xodusTx, false)
        }
        return null
    }

    /**
     * Convenience method to update [IndexState] for this [AbstractIndex].
     *
     * @param state The new [IndexState].
     * @param indexTx The [ [AbstractIndex.Tx]] to use.
     */
    private fun updateState(state: IndexState, indexTx: AbstractIndex.Tx): Boolean {
        val name = NameBinding.Index.toEntry(this@AbstractIndexRebuilder.index.name)
        val store = IndexMetadata.store(indexTx.parent.parent.parent.xodusTx)
        val oldEntryRaw = store.get(indexTx.parent.parent.parent.xodusTx, name) ?: throw DatabaseException.DataCorruptionException("Failed to rebuild index transaction for index ${this@AbstractIndexRebuilder.index.name}: Could not read catalogue entry for index.")
        val oldEntry =  IndexMetadata.fromEntry(oldEntryRaw)
        return if (oldEntry.state != state) {
            return store.put(indexTx.parent.parent.parent.xodusTx, name, IndexMetadata.toEntry(oldEntry.copy(state = state)))
        } else {
            true
        }
    }
}