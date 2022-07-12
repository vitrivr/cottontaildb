package org.vitrivr.cottontail.dbms.index.pq.rebuilder

import org.apache.commons.math3.random.JDKRandomGenerator
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.values.types.VectorValue
import org.vitrivr.cottontail.dbms.column.ColumnTx
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.index.pq.PQIndexConfig
import java.util.*
import kotlin.math.log10

/**
 *
 */
object PQIndexRebuilderUtilites {
    /**
     * Collects and returns a subset of the available data for learning and training.
     *
     * @param txn The [EntityTx] used to obtain the learning data.
     * @return List of [Record]s used for learning.
     */
    fun acquireLearningData(txn: ColumnTx<*>, config: PQIndexConfig): List<VectorValue<*>> {
        val count = txn.count()
        if (count == 0L) return emptyList()
        val random = JDKRandomGenerator(config.seed)
        val learningData = LinkedList<VectorValue<*>>()
        val learningDataFraction = (1000.0f * log10(count.toDouble())).toInt()
        txn.cursor().use { cursor ->
            while (cursor.hasNext()) {
                if (random.nextDouble() <= learningDataFraction) {
                    val value = cursor.value()
                    if (value is VectorValue<*>) {
                        learningData.add(value)
                    }
                }
            }
        }
        return learningData
    }
}