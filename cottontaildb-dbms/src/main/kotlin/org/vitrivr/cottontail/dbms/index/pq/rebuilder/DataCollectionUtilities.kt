package org.vitrivr.cottontail.dbms.index.pq.rebuilder

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.dbms.entity.EntityTx
import java.util.*

/**
 * A collection of utility classes used for data collection.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object DataCollectionUtilities {
    /**
     * Collects and returns a subset of the available data for learning and training.
     *
     * @param txn The [EntityTx] used to obtain the learning data.
     * @param column The [ColumnDef] used to obtain the learning data.
     * @param fraction The fraction of the data to select.
     * @param seed The seed for the random number generator.
     * @return List of [Tuple]s used for learning.
     */
    fun acquireLearningData(txn: EntityTx, column: ColumnDef<*>, fraction: Float, seed: Long): List<VectorValue<*>> {
        val random = SplittableRandom(seed)
        val learningData = LinkedList<VectorValue<*>>()
        txn.cursor(arrayOf(column)).use { cursor ->
            while (cursor.hasNext()) {
                if (random.nextDouble() <= fraction) {
                    val value = cursor.value()[0]
                    if (value is VectorValue<*>) {
                        learningData.add(value)
                    }
                }
            }
        }
        return learningData
    }
}