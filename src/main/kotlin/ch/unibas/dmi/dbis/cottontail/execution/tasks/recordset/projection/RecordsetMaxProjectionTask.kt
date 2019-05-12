package ch.unibas.dmi.dbis.cottontail.execution.tasks.recordset.projection

import ch.unibas.dmi.dbis.cottontail.database.queries.Projection
import ch.unibas.dmi.dbis.cottontail.execution.cost.Costs
import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.model.values.DoubleValue

import com.github.dexecutor.core.task.Task
import com.github.dexecutor.core.task.TaskExecutionException

/**
 * A [Task] used during query execution. It takes a single [Recordset] and determines the maximum value of a specific [ColumnDef]. It thereby creates a 1x1 [Recordset].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class RecordsetMaxProjectionTask(val projection: Projection, estimatedSize: Int = 1000): ExecutionTask("RecordsetMaxProjectionTask") {

    /** The cost of this [RecordsetMaxProjectionTask] is constant */
    override val cost = estimatedSize * Costs.MEMORY_ACCESS_READ

    /**
     * Executes this [RecordsetCountProjectionTask]
     */
    override fun execute(): Recordset {
        assertUnaryInput()

        /* Get records from parent task. */
        val parent = this.first() ?: throw TaskExecutionException("Projection could not be executed because parent task has failed.")

        /* Calculate max(). */
        val column = projection.columns.first()
        val resultsColumn = ColumnDef.withAttributes(this.projection.fields[column.name] ?: "max(${column.name})", "DOUBLE")
        var max = 0.0
        val results = Recordset(arrayOf(resultsColumn))
        parent.forEach {
            when (val value = it[column]?.value) {
                is Byte -> max = Math.max(max, value.toDouble())
                is Short -> max = Math.max(max, value.toDouble())
                is Int -> max = Math.max(max, value.toDouble())
                is Long -> max = Math.max(max, value.toDouble())
                is Float -> max = Math.max(max, value.toDouble())
                is Double -> max = Math.max(max, value)
                else -> {}
            }
        }
        results.addRowUnsafe(arrayOf(DoubleValue(max)))
        return results
    }
}