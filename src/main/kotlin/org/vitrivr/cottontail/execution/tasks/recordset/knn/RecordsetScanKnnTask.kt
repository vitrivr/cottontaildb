package org.vitrivr.cottontail.execution.tasks.recordset.knn

import com.github.dexecutor.core.task.Task
import com.github.dexecutor.core.task.TaskExecutionException
import org.vitrivr.cottontail.database.queries.components.BooleanPredicate
import org.vitrivr.cottontail.database.queries.components.KnnPredicate
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionTask
import org.vitrivr.cottontail.execution.tasks.entity.knn.KnnUtilities
import org.vitrivr.cottontail.math.knn.selection.ComparablePair
import org.vitrivr.cottontail.math.knn.selection.MinHeapSelection
import org.vitrivr.cottontail.math.knn.selection.MinSingleSelection
import org.vitrivr.cottontail.math.knn.selection.Selection
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.VectorValue
import org.vitrivr.cottontail.utilities.name.Name

/**
 * A [Task] that executes a sequential kNN on a [Column][org.vitrivr.cottontail.database.column.Column]
 * of the input [Recordset] and returns the results of the kNN lookup.
 *
 * @author Ralph Gasser
 * @version 1.2
 */
class RecordsetScanKnnTask<T : VectorValue<*>>(val knn: KnnPredicate<T>, val predicate: BooleanPredicate? = null) : ExecutionTask("RecordsetScanKnnTask[${knn.column.name}][${knn.distance::class.simpleName}][${knn.k}][q=${knn.query.hashCode()}]") {
    /** Set containing the kNN values. */
    private val knnSet: List<Selection<ComparablePair<Long, DoubleValue>>> = if (this.knn.k == 1) {
        this.knn.query.map { MinSingleSelection<ComparablePair<Long, DoubleValue>>() }
    } else {
        this.knn.query.map { MinHeapSelection<ComparablePair<Long, DoubleValue>>(this.knn.k) }
    }

    /** The output [ColumnDef] produced by this [RecordsetScanKnnTask]. */
    private val column = ColumnDef(Name(KnnUtilities.DISTANCE_COLUMN_NAME), KnnUtilities.DISTANCE_COLUMN_TYPE)

    /**
     * Executes this [EntityScanKnnTask]
     */
    override fun execute(): Recordset {
        /* Get records from parent task. */
        val parent = this.first()
                ?: throw TaskExecutionException("Recordset  projection could not be executed because parent task has failed.")

        /* Prepare scan action. */
        val action: (Record) -> Unit = if (this.knn.weights != null) {
            {
                val value = it[this.knn.column]
                if (value != null) {
                    this.knn.query.forEachIndexed { i, query ->
                        this.knnSet[i].offer(ComparablePair(it.tupleId, this.knn.distance(query, value, this.knn.weights[i])))
                    }
                }
            }
        } else {
            {
                val value = it[this.knn.column]
                if (value != null) {
                    this.knn.query.forEachIndexed { i, query ->
                        this.knnSet[i].offer(ComparablePair(it.tupleId, this.knn.distance(query, value)))
                    }
                }
            }
        }

        /* Execute table scan. */
        if (this.predicate != null) {
            parent.forEach(this.predicate, action)
        } else {
            parent.forEach(action)
        }

        /** Generate recordset from HeapSelect data structures. */
        return KnnUtilities.selectToRecordset(this.column, this.knnSet)
    }
}