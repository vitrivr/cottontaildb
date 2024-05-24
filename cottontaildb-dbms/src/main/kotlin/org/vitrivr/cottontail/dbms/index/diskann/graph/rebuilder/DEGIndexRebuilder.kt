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
        val column = indexTx.columns[0]

        /* Generate Graph structure. */
        val graph = DefaultDynamicExplorationGraph<VectorValue<*>>(config, indexTx)

        /* Iterate over column and update index with entries. */
        try {
            indexTx.parent.cursor(indexTx.columns).use { cursor ->
                while (cursor.moveNext()) {
                    val value = cursor.value()[column]
                    if (value is VectorValue<*>) {
                        graph.index(cursor.key(), value)
                    }
                }
            }

            /* Saves the graph. */
            graph.save()
        } catch (e: Throwable) {
            return false
        }


        /* Update stored ProductQuantizer. */
        return true
    }
}