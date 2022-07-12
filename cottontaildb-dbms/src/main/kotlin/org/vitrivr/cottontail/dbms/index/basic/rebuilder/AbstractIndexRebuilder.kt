package org.vitrivr.cottontail.dbms.index.basic.rebuilder

import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexCatalogueEntry
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.index.basic.IndexState
import org.vitrivr.cottontail.dbms.index.va.VAFIndex

/**
 * An abstract [IndexRebuilder] implementation.
 *
 * @see [IndexRebuilder]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class AbstractIndexRebuilder<T: Index>(final override val index: T,
                                                final override val context: TransactionContext): IndexRebuilder<T> {

    companion object {
        /** [Logger] instance used by [AbstractIndexRebuilder]. */
        protected val LOGGER: Logger = LoggerFactory.getLogger(AbstractIndexRebuilder::class.java)
    }

    /**
     * Starts the index rebuilding process for this [AbstractIndexRebuilder].
     *
     * @return True on success, false otherwise.
     */
    @Synchronized
    override fun rebuild(): Boolean {
        /* Sanity check. */
        require(context.xodusTx.isExclusive) { "Failed to rebuild index ${this.index.name} (${this.index.type}); rebuild operation requires exclusive transaction."}

        VAFIndex.LOGGER.debug("Rebuilding index ${this.index.name} (${this.index.type}).")

        /* Clear store and update state of index (* ---> DIRTY). */
        val dataStore: Store = this.clearAndOpenStore()
        if (!IndexCatalogueEntry.updateState(this.index.name, this.index.catalogue as DefaultCatalogue, IndexState.DIRTY, context.xodusTx)) {
            LOGGER.error("Rebuilding index ${this.index.name} (${this.index.type}) failed because index state could not be changed to CLEAN!")
            return false
        }

        /* Execute rebuild operation*/
        if (this.rebuildInternal(dataStore)) {
            LOGGER.error("Rebuilding index ${this.index.name} (${this.index.type}) failed!")
            return false
        }

        /* Update state of index (DIRTY ---> CLEAN). */
        if (!IndexCatalogueEntry.updateState(this.index.name, this.index.catalogue as DefaultCatalogue, IndexState.CLEAN, context.xodusTx)) {
            LOGGER.error("Rebuilding index ${this.index.name} (${this.index.type}) failed because index state could not be changed to CLEAN!")
            return false
        }

        LOGGER.debug("Rebuilding index ${this.index.name} (${this.index.type}) completed!")
        return true
    }

    /**
     * Internal implementation of the actual re-indexing procedure.
     *
     * @param dataStore The [Store] that contains the [Index] data.
     * @return True on success, false otherwise.
     */
    protected abstract fun rebuildInternal(dataStore: Store): Boolean

    /**
     * Clears and opens the data store associated with this [Index].
     *
     * @return [Store]
     */
    private fun clearAndOpenStore(): Store {
        val storeName = this.index.name.storeName()
        (this.index.catalogue as DefaultCatalogue).environment.truncateStore(storeName, this.context.xodusTx)
        return (this.index.catalogue as DefaultCatalogue).environment.openStore(storeName, StoreConfig.USE_EXISTING, context.xodusTx, false)
            ?: throw DatabaseException.DataCorruptionException("Data store for index ${this.index.name} is missing.")
    }
}