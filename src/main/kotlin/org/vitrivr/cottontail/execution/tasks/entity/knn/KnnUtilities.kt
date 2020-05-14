package org.vitrivr.cottontail.execution.tasks.entity.knn

import org.vitrivr.cottontail.database.column.DoubleColumnType
import org.vitrivr.cottontail.math.knn.ComparablePair
import org.vitrivr.cottontail.math.knn.HeapSelect
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.values.DoubleValue

/**
 * Utilities and constants used for nearest neighbor search.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
object KnnUtilities {

    /** Desired number of operations for a kNN lookup per [ExecutionTask]. */
    const val OPERATIONS_PER_TASK = 250000.0

    /** Name of the distance column produced by a kNN [ExecutionTask]. */
    const val DISTANCE_COLUMN_NAME = "distance"

    /** Type of the distance column produced by a kNN [ExecutionTask]. */
    val DISTANCE_COLUMN_TYPE = DoubleColumnType

    /**
     * Combines a [Collection] of [HeapSelect] data structures into a [Recordset].
     *
     * @param column The [ColumnDef] of the new [Recordset]
     * @param list List of [HeapSelect]s to combine.
     * @return [Recordset]
     */
    fun heapSelectToRecordset(column: ColumnDef<*>, list: List<HeapSelect<ComparablePair<Long, DoubleValue>>>): Recordset {
        val dataset = Recordset(arrayOf(column), capacity = (list.size * list.first().size).toLong())
        for (knn in list) {
            for (i in 0 until knn.size) {
                dataset.addRowUnsafe(knn[i].first, arrayOf(DoubleValue(knn[i].second)))
            }
        }
        return dataset
    }
}