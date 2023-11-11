package org.vitrivr.cottontail.dbms.index.pq.rebuilder

import jetbrains.exodus.bindings.ShortBinding
import org.vitrivr.cottontail.core.queries.functions.Argument
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.VectorValue
import org.vitrivr.cottontail.dbms.execution.transactions.AccessMode
import org.vitrivr.cottontail.dbms.index.basic.DefaultIndex
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AbstractIndexRebuilder
import org.vitrivr.cottontail.dbms.index.pq.IVFPQIndex
import org.vitrivr.cottontail.dbms.index.pq.IVFPQIndexConfig
import org.vitrivr.cottontail.dbms.index.pq.quantizer.MultiStageQuantizer
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * An [AbstractIndexRebuilder] for the [IVFPQIndex].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class IVFPQIndexRebuilder(index: IVFPQIndex, context: QueryContext): AbstractIndexRebuilder<IVFPQIndex>(index, context) {

    /**
     * Starts the index rebuilding process for this [PQIndexRebuilder].
     *
     * @return True on success, false on failure.
     */
    override fun rebuildInternal(indexTx: DefaultIndex.Tx): Boolean {
        require(indexTx is IVFPQIndex.Tx) { "PQIndexRebuilder only supports PQIndex.Tx implementations." }

        /* Tx objects required for index rebuilding. */
        val entityTx = this.context.transaction.entityTx(indexTx.dbo.parent.name, AccessMode.READ)
        val columnTx = this.context.transaction.columnTx(indexTx.columns[0].name, AccessMode.READ)
        val count = entityTx.count()

        /* Obtain PQ data structure. */
        val config = indexTx.config as IVFPQIndexConfig
        val type = columnTx.columnDef.type as Types.Vector<*,*>
        val distanceFunction = this.index.catalogue.functions.obtain(Signature.SemiClosed(config.distance, arrayOf(Argument.Typed(type), Argument.Typed(type)))) as VectorDistance<*>
        val fraction = ((3.0f * config.numCentroids) / count)
        val seed = System.currentTimeMillis()
        val learningData = DataCollectionUtilities.acquireLearningData(columnTx, fraction, seed)
        val quantizer = MultiStageQuantizer.learnFromData(distanceFunction, learningData, config)

        /* Iterate over column and update index with entries. */
        columnTx.cursor().use { cursor ->
            while (cursor.moveNext()) {
                val value = cursor.value()
                if (value is VectorValue<*>) {
                    val signature = quantizer.quantize(cursor.key(), value)
                    if (!indexTx.store.put(indexTx.xodusTx, ShortBinding.shortToEntry(signature.first), signature.second.toEntry())) {
                        return false
                    }
                }
            }
        }

        /* Update quantizer and return. */
        indexTx.updateQuantizer(quantizer.toSerializableProductQuantizer())
        return true
    }
}