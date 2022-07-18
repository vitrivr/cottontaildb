package org.vitrivr.cottontail.dbms.index.pq.rebuilder

import org.apache.commons.math3.random.JDKRandomGenerator
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.values.types.VectorValue
import org.vitrivr.cottontail.dbms.column.ColumnTx
import java.util.*

/**
 *
 */
object PQIndexRebuilderUtilites {
    /**
     * Collects and returns a subset of the available data for learning and training.
     *
     * @param txn The [ColumnTx] used to obtain the learning data.
     * @param centroids The planned number of centroids.
     * @param seed The seed for the random number generator.
     * @return List of [Record]s used for learning.
     */
    fun acquireLearningData(txn: ColumnTx<*>, centroids: Int, seed: Int): List<VectorValue<*>> {
        val count = txn.count()
        if (count == 0L) return emptyList()
        val random = JDKRandomGenerator(seed)
        val learningData = LinkedList<VectorValue<*>>()
        val learningDataFraction = ((3.0 * centroids) / count)
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