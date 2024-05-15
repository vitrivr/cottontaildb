package org.vitrivr.cottontail.dbms.index.diskann.graph.rebuilder

import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.dbms.index.basic.AbstractIndex
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractIndexRebuilder
import org.vitrivr.cottontail.dbms.index.diskann.graph.DEGIndex
import org.vitrivr.cottontail.dbms.index.diskann.graph.DEGIndexConfig
import org.vitrivr.cottontail.dbms.index.diskann.graph.deg.DefaultDynamicExplorationGraph
import org.vitrivr.cottontail.dbms.index.pq.rebuilder.PQIndexRebuilder
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * An [AbstractIndexRebuilder] for the [DEGIndex].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class DEGIndexRebuilder(index: DEGIndex, context: QueryContext): AbstractIndexRebuilder<DEGIndex>(index, context) {

    /**
     * Starts the index rebuilding process for this [PQIndexRebuilder].
     *
     * @return True on success, false on failure.
     */
    override fun rebuildInternal(indexTx: AbstractIndex.Tx): Boolean {
        require(indexTx is DEGIndex.Tx) { "DEGIndexRebuilder can only be accessed with a DEGIndex.Tx!" }

        /* Read basic index properties. */
        val config = indexTx.config as DEGIndexConfig

        /* Open data store. */
        this.tryClearAndOpenStore(indexTx) ?: return false
        val count = indexTx.parent.count()

        /* Generate Graph structure. */
        val graph = DefaultDynamicExplorationGraph<VectorValue<*>>(config, indexTx)

        /* Iterate over column and update index with entries. */
        var counter = 0
        indexTx.parent.cursor(indexTx.columns).use { cursor ->
            while (cursor.moveNext()) {
                val value = cursor.value()[0]
                if (value is VectorValue<*>) {
                    graph.index(cursor.key(), value)

                    /* Data is flushed every once in a while. */
                    if ((++counter) % 1_000_000 == 0) {
                        LOGGER.debug("Rebuilding index {} ({}) still running ({} / {})...", this.index.name, this.index.type, counter, count)
                    }
                }
            }
        }
        return true
    }
}