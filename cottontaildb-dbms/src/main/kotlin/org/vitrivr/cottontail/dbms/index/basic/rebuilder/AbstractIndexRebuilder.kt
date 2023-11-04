package org.vitrivr.cottontail.dbms.index.basic.rebuilder

import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.Transaction
import org.vitrivr.cottontail.dbms.index.basic.AbstractIndex
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.index.basic.IndexMetadata
import org.vitrivr.cottontail.dbms.index.basic.IndexState
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * An abstract [IndexRebuilder] implementation.
 *
 * @see [IndexRebuilder]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class AbstractIndexRebuilder<T: Index>(final override val index: T,
                                                final override val context: QueryContext): IndexRebuilder<T> {
    companion object {
        /** [Logger] instance used by [AbstractIndexRebuilder]. */
        internal val LOGGER: Logger = LoggerFactory.getLogger(AbstractIndexRebuilder::class.java)
    }

    /**
     * Starts the index rebuilding process for this [AbstractIndexRebuilder].
     *
     * @return True on success, false otherwise.
     */
    @Synchronized
    override fun rebuild(): Boolean {
        /* Sanity check. */
        require(this.context.txn.xodusTx.isExclusive) { "Failed to rebuild index ${this.index.name} (${this.index.type}); rebuild operation requires exclusive transaction."}

        LOGGER.debug("Rebuilding index {} ({}).", this.index.name, this.index.type)

        /* Clear store and update state of index (* ---> DIRTY). */
        if (!this.updateState(IndexState.DIRTY, this.context.txn)) {
            LOGGER.error("Rebuilding index ${this.index.name} (${this.index.type}) failed because index state could not be changed to CLEAN!")
            return false
        }

        /* Execute rebuild operation*/
        if (!this.rebuildInternal()) {
            LOGGER.error("Rebuilding index ${this.index.name} (${this.index.type}) failed!")
            return false
        }

        /* Update state of index (DIRTY ---> CLEAN). */
        if (!this.updateState( IndexState.CLEAN, this.context.txn)) {
            LOGGER.error("Rebuilding index ${this.index.name} (${this.index.type}) failed because index state could not be changed to CLEAN!")
            return false
        }

        LOGGER.debug("Rebuilding index {} ({}) completed!", this.index.name, this.index.type)
        return true
    }

    /**
     * Internal implementation of the actual re-indexing procedure.
     *
     * @return True on success, false otherwise.
     */
    protected abstract fun rebuildInternal(): Boolean

    /**
     * Clears and opens the data store associated with this [Index].
     *
     * @return [Store]
     */
    protected fun tryClearAndOpenStore(): Store? {
        val storeName = this.index.name.storeName()
        if (this.index.catalogue.transactionManager.environment.storeExists(storeName, this.context.txn.xodusTx)) {
            this.index.catalogue.transactionManager.environment.truncateStore(storeName, this.context.txn.xodusTx)
            return this.index.catalogue.transactionManager.environment.openStore(storeName, StoreConfig.USE_EXISTING, this.context.txn.xodusTx, false)
        }
        return null
    }

    /**
     * Convenience method to update [IndexState] for this [AbstractIndex].
     *
     * @param state The new [IndexState].
     * @param tx The [Transaction] to use.
     */
    private fun updateState(state: IndexState, tx: Transaction): Boolean {
        val name = NameBinding.Index.toEntry(this@AbstractIndexRebuilder.index.name)
        val store = IndexMetadata.store(this@AbstractIndexRebuilder.index.catalogue as DefaultCatalogue, tx.xodusTx)
        val oldEntryRaw = store.get(tx.xodusTx, name) ?: throw DatabaseException.DataCorruptionException("Failed to rebuild index transaction for index ${this@AbstractIndexRebuilder.index.name}: Could not read catalogue entry for index.")
        val oldEntry =  IndexMetadata.fromEntry(oldEntryRaw)
        return if (oldEntry.state != state) {
            store.put(tx.xodusTx, name, IndexMetadata.toEntry(oldEntry.copy(state = state)))
        } else {
            false
        }
    }
}