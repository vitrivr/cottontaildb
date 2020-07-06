package org.vitrivr.cottontail.execution.tasks.recordset.projection

import com.github.dexecutor.core.task.Task
import com.github.dexecutor.core.task.TaskExecutionException
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionTask
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.values.*
import kotlin.math.max

/**
 * A [Task] used during query execution. It takes a single [Recordset] and determines the maximum value of a specific [ColumnDef]. It thereby creates a 1x1 [Recordset].
 *
 * @author Ralph Gasser
 * @version 1.1
 */
class RecordsetMaxProjectionTask(val columns: Array<ColumnDef<*>>, val fields: Map<Name.ColumnName, Name.ColumnName?>) : ExecutionTask("RecordsetMaxProjectionTask") {

    /**
     * Executes this [RecordsetCountProjectionTask]
     */
    override fun execute(): Recordset {
        assertUnaryInput()

        /* Get records from parent task. */
        val parent = this.first()
                ?: throw TaskExecutionException("MAX projection could not be executed because parent task has failed.")

        /* Calculate max(). */
        val column = this.columns.first()
        val resultsColumn = ColumnDef.withAttributes(this.fields[column.name]
                ?: (column.name.entity()?.column("max(${column.name})") ?: Name.ColumnName("max(${column.name})")), "DOUBLE")
        var max = Double.MIN_VALUE
        val results = Recordset(arrayOf(resultsColumn))
        parent.forEach {
            when (val value = it[column]) {
                is ByteValue -> max = max(max, value.value.toDouble())
                is ShortValue -> max = max(max, value.value.toDouble())
                is IntValue -> max = max(max, value.value.toDouble())
                is LongValue -> max = max(max, value.value.toDouble())
                is FloatValue -> max = max(max, value.value.toDouble())
                is DoubleValue -> max = max(max, value.value)
                else -> {}
            }
        }
        results.addRowUnsafe(arrayOf(DoubleValue(max)))
        return results
    }
}