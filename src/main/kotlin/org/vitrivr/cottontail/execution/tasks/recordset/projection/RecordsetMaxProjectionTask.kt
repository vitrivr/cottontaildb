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
 * A [Task] used during query execution. It takes a single [Recordset] and determines the maximum value of a specific [ColumnDef]. It thereby creates a 1x1 [Recordset].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class RecordsetMaxProjectionTask(val projection: Projection, estimatedRows: Int = 1000) : ExecutionTask("RecordsetMaxProjectionTask") {

    /** The cost of this [RecordsetMaxProjectionTask]  depends on the estimated size of the input. */
    override val cost = estimatedRows * Costs.MEMORY_ACCESS_READ

    init {
        assert(projection.type == ProjectionType.MAX)
    }

    /**
     * Executes this [RecordsetCountProjectionTask]
     */
    override fun execute(): Recordset {
        assertUnaryInput()

        /* Get records from parent task. */
        val parent = this.first()
                ?: throw TaskExecutionException("MAX projection could not be executed because parent task has failed.")

        /* Calculate max(). */
        val column = projection.columns.first()
        val resultsColumn = ColumnDef.withAttributes(this.projection.fields[column.name]
                ?: Name("max(${column.name})"), "DOUBLE")
        var max = Double.MIN_VALUE
        val results = Recordset(arrayOf(resultsColumn))
        parent.forEach {
            when (val value = it[column]?.value) {
                is Byte -> max = Math.max(max, value.toDouble())
                is Short -> max = Math.max(max, value.toDouble())
                is Int -> max = Math.max(max, value.toDouble())
                is Long -> max = Math.max(max, value.toDouble())
                is Float -> max = Math.max(max, value.toDouble())
                is Double -> max = Math.max(max, value)
                else -> {
                }
            }
        }
        results.addRowUnsafe(arrayOf(DoubleValue(max)))
        return results
    }
}