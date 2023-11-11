package org.vitrivr.cottontail.dbms.index.basic.rebuilder

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.dbms.execution.transactions.AccessMode
import org.vitrivr.cottontail.dbms.index.basic.DefaultIndex
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.index.basic.IndexState
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
        internal val LOGGER: Logger = LoggerFactory.getLogger(AbstractIndexRebuilder::class.java)
    }

    /**
     * Starts the index rebuilding process for this [AbstractIndexRebuilder].
     *
     * @return True on success, false otherwise.
     */
    @Synchronized
    override fun rebuild(): Boolean {
        LOGGER.debug("Rebuilding index {} ({}).", this.index.name, this.index.type)

        /* Obtain index transaction. */
        val indexTx = this.context.transaction.indexTx(this.index.name, AccessMode.WRITE)
        require(indexTx is DefaultIndex.Tx) { "AbstractIndexRebuilder only supports DefaultIndex.Tx implementations." }

        /* Truncate index. */
        indexTx.truncate()

        /* Execute rebuild operation*/
        if (!this.rebuildInternal(indexTx)) {
            LOGGER.error("Rebuilding index ${this.index.name} (${this.index.type}) failed!")
            return false
        }

        /* Set index to stale. */
        indexTx.updateState(IndexState.CLEAN)

        LOGGER.debug("Rebuilding index {} ({}) completed!", this.index.name, this.index.type)
        return true
    }

    /**
     * Internal implementation of the actual re-indexing procedure.
     *
     * @return True on success, false otherwise.
     */
    protected abstract fun rebuildInternal(indexTx: DefaultIndex.Tx): Boolean
}