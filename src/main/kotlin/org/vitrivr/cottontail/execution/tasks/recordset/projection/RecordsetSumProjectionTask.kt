package org.vitrivr.cottontail.execution.tasks.recordset.projection

import com.github.dexecutor.core.task.Task
import com.github.dexecutor.core.task.TaskExecutionException
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionTask
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.values.*

/**
 * A [Task] used during query execution. It takes a single [Recordset] and determines the sum of a specific [ColumnDef]. It thereby creates a 1x1 [Recordset].
 *
 * @author Ralph Gasser
 * @version 1.1
 */
class RecordsetSumProjectionTask(val columns: Array<ColumnDef<*>>, val fields: Map<Name.ColumnName, Name.ColumnName?>) : ExecutionTask("RecordsetSumProjectionTask") {

    /**
     * Executes this [RecordsetCountProjectionTask]
     */
    override fun execute(): Recordset {
        assertUnaryInput()

        /* Get records from parent task. */
        val parent = this.first()
                ?: throw TaskExecutionException("SUM projection could not be executed because parent task has failed.")

        /* Calculate sum(). */
        val column = this.columns.first()
        val resultsColumn = ColumnDef.withAttributes(this.fields[column.name]
                ?: (column.name.entity()?.column("max(${column.name})") ?: Name.ColumnName("max(${column.name})")), "DOUBLE")
        var sum = 0.0
        val results = Recordset(arrayOf(resultsColumn))
        parent.forEach {
            when (val value = it[column]) {
                is ByteValue -> sum += value.value
                is ShortValue -> sum += value.value
                is IntValue -> sum += value.value
                is LongValue -> sum += value.value
                is FloatValue -> sum += value.value
                is DoubleValue -> sum += value.value
                else -> {}
            }
        }
        results.addRowUnsafe(arrayOf(DoubleValue(sum)))
        return results
    }
}