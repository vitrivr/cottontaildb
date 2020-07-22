package org.vitrivr.cottontail.execution.tasks.recordset.merge

import com.github.dexecutor.core.task.Task
import com.github.dexecutor.core.task.TaskExecutionException
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.components.KnnPredicate
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionTask
import org.vitrivr.cottontail.execution.tasks.entity.knn.KnnUtilities
import org.vitrivr.cottontail.math.knn.selection.ComparablePair
import org.vitrivr.cottontail.math.knn.selection.MinHeapSelection
import org.vitrivr.cottontail.math.knn.selection.MinSingleSelection
import org.vitrivr.cottontail.math.knn.selection.Selection
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.values.DoubleValue

/**
 * A [Task] that executes a sequential kNN on a float [Column][org.vitrivr.cottontail.database.column.Column] of the specified [Entity].
 *
 * @author Ralph Gasser
 * @version 1.1.1
 */
class RecordsetMergeKnnTask(val entity: Entity, val knn: KnnPredicate<*>) : ExecutionTask("RecordsetMergeKnnTask[${knn.k}]") {
    /** Set containing the kNN values. */
    private val knnSet: List<Selection<ComparablePair<Long, DoubleValue>>> = if (this.knn.k == 1) {
        this.knn.query.map { MinSingleSelection<ComparablePair<Long, DoubleValue>>() }
    } else {
        this.knn.query.map { MinHeapSelection<ComparablePair<Long, DoubleValue>>(this.knn.k) }
    }

    /** The output [ColumnDef] produced by this [RecordsetMergeKnnTask]. */
    private val column = ColumnDef(this.entity.fqn.append(KnnUtilities.DISTANCE_COLUMN_NAME), KnnUtilities.DISTANCE_COLUMN_TYPE)

    override fun execute(): Recordset {
        val input = this.allSuccessful()

        /* Merge kNN values for the given recordsets. */
        for (i in input) {
            i.forEachIndexed { j, r ->
                val value = r[this.column]
                        ?: throw TaskExecutionException("Recordset kNN MERGE could not be executed because recordset does not seem to contain a valid `${column.name}` column.")
                knnSet[j / knn.k].offer(ComparablePair(r.tupleId, value))
            }
        }

        /** Generate record set from Selection data structures. */
        return KnnUtilities.selectToRecordset(this.column, this.knnSet)
    }
}

