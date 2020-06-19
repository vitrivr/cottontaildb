package org.vitrivr.cottontail.execution.tasks.entity.knn

import org.vitrivr.cottontail.database.column.DoubleColumnType
import org.vitrivr.cottontail.math.knn.selection.ComparablePair
import org.vitrivr.cottontail.math.knn.selection.MinHeapSelection
import org.vitrivr.cottontail.math.knn.selection.Selection
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

    /** Name of the distance column produced by a kNN [ExecutionTask]. */
    const val DISTANCE_COLUMN_NAME = "distance"

    /** Type of the distance column produced by a kNN [ExecutionTask]. */
    val DISTANCE_COLUMN_TYPE = DoubleColumnType

    /**
     * Combines a [Collection] of [MinHeapSelection] data structures into a [Recordset].
     *
     * @param column The [ColumnDef] of the new [Recordset]
     * @param list List of [MinHeapSelection]s to combine.
     * @return [Recordset]
     */
    fun selectToRecordset(column: ColumnDef<*>, list: List<Selection<ComparablePair<Long, DoubleValue>>>): Recordset {
        val dataset = Recordset(arrayOf(column), capacity = (list.size * list.first().size).toLong())
        for (knn in list) {
            for (i in 0 until knn.size) {
                dataset.addRowUnsafe(knn[i].first, arrayOf(DoubleValue(knn[i].second)))
            }
        }
        return dataset
    }
}