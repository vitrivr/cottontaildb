package org.vitrivr.cottontail.execution.tasks.recordset.projection

import com.github.dexecutor.core.task.Task
import com.github.dexecutor.core.task.TaskExecutionException
import org.vitrivr.cottontail.database.queries.Projection
import org.vitrivr.cottontail.database.queries.ProjectionType
import org.vitrivr.cottontail.execution.cost.Costs
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionTask
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.utilities.name.Name

/**
 * A [Task] used during query execution. It takes a single [Recordset] and determines the mean of a specific [ColumnDef]. It thereby creates a 1x1 [Recordset].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class RecordsetMeanProjectionTask(val projection: Projection, estimatedRows: Int = 1000) : ExecutionTask("RecordsetMeanProjectionTask") {

    /** The cost of this [RecordsetMaxProjectionTask]  depends on the estimated size of the input. */
    override val cost = estimatedRows * Costs.MEMORY_ACCESS_READ

    init {
        assert(projection.type == ProjectionType.MEAN)
    }

    /**
     * Executes this [RecordsetCountProjectionTask]
     */
    override fun execute(): Recordset {
        assertUnaryInput()

        /* Get records from parent task. */
        val parent = this.first()
                ?: throw TaskExecutionException("MEAN projection could not be executed because parent task has failed.")

        /* Calculate mean(). */
        val column = projection.columns.first()
        val resultsColumn = ColumnDef.withAttributes(this.projection.fields[column.name]
                ?: Name("mean(${column.name})"), "DOUBLE")
        val results = Recordset(arrayOf(resultsColumn))
        if (parent.rowCount > 0) {
            var sum = 0.0
            parent.forEach {
                when (val value = it[column]?.value) {
                    is Byte -> sum += value
                    is Short -> sum += value
                    is Int -> sum += value
                    is Long -> sum += value
                    is Float -> sum += value
                    is Double -> sum += value
                    else -> {
                    }
                }
            }
            results.addRowUnsafe(arrayOf(DoubleValue(sum / parent.rowCount)))
            return results
        } else {
            results.addRowUnsafe(arrayOf(DoubleValue(0.0)))
            return results
        }
    }
}