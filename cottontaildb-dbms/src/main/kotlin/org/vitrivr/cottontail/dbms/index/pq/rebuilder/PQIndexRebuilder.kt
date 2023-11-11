package org.vitrivr.cottontail.dbms.index.pq.rebuilder

import org.vitrivr.cottontail.core.values.VectorValue
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.execution.transactions.AccessMode
import org.vitrivr.cottontail.dbms.index.basic.DefaultIndex
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractIndexRebuilder
import org.vitrivr.cottontail.dbms.index.pq.PQIndex
import org.vitrivr.cottontail.dbms.index.pq.PQIndexConfig
import org.vitrivr.cottontail.dbms.index.pq.quantizer.SingleStageQuantizer
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * An [AbstractIndexRebuilder] for the [PQIndex].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class PQIndexRebuilder(index: PQIndex, context: QueryContext): AbstractIndexRebuilder<PQIndex>(index, context) {
    /**
     * Starts the index rebuilding process for this [PQIndexRebuilder].
     *
     * @return True on success, false on failure.
     */
    override fun rebuildInternal(indexTx: DefaultIndex.Tx): Boolean {
        require(indexTx is PQIndex.Tx) { "PQIndexRebuilder only supports PQIndex.Tx implementations." }

        /* Tx objects required for index rebuilding. */
        val entityTx = this.context.transaction.entityTx(indexTx.dbo.parent.name, AccessMode.READ)
        val columnTx = this.context.transaction.columnTx(indexTx.columns[0].name, AccessMode.READ)
        val count = entityTx.count()

        /* Obtain PQ data structure. */
        val config = indexTx.config as PQIndexConfig
        val distanceFunction = indexTx.distanceFunction
        val fraction = ((3.0f * config.numCentroids) / count)
        val seed = System.currentTimeMillis()
        val learningData = DataCollectionUtilities.acquireLearningData(columnTx, fraction, seed)
        val quantizer = SingleStageQuantizer.learnFromData(distanceFunction, learningData, config)

        /* Iterate over entity and update index with entries. */
        var counter = 0
        val store = indexTx.store
        columnTx.cursor().use { cursor ->
            while (cursor.moveNext()) {
                val value = cursor.value()
                if (value is VectorValue<*>) {
                    if (!store.put(indexTx.xodusTx, cursor.key().toKey(), quantizer.quantize(value).toEntry())) {
                        return false
                    }

                    /* Data is flushed every once in a while. */
                    if ((++counter) % 1_000_000 == 0) {
                        LOGGER.debug("Rebuilding index {} ({}) still running ({} / {})...", this.index.name, this.index.type, counter, count)
                        if (!indexTx.xodusTx.flush()) {
                            return false
                        }
                    }
                }
            }
        }

        /* Update quantizer and return. */
        indexTx.updateQuantizer(quantizer.toSerializableProductQuantizer())
        return true
    }
}