package org.vitrivr.cottontail.execution.tasks.recordset.projection

import com.github.dexecutor.core.task.Task
import com.github.dexecutor.core.task.TaskExecutionException
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionTask
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.values.*

/**
 * A [Task] used during query execution. It takes a single [Recordset] and determines the mean of a specific [ColumnDef]. It thereby creates a 1x1 [Recordset].
 *
 * @author Ralph Gasser
 * @version 1.1
 */
class RecordsetMeanProjectionTask(val columns: Array<ColumnDef<*>>, val fields: Map<Name.ColumnName, Name.ColumnName?>) : ExecutionTask("RecordsetMeanProjectionTask") {
    /**
     * Executes this [RecordsetCountProjectionTask]
     */
    override fun execute(): Recordset {
        assertUnaryInput()

        /* Get records from parent task. */
        val parent = this.first()
                ?: throw TaskExecutionException("MEAN projection could not be executed because parent task has failed.")

        /* Calculate mean(). */
        val column = this.columns.first()
        val resultsColumn = ColumnDef.withAttributes(this.fields[column.name]
                ?: (column.name.entity()?.column("mean(${column.name})") ?: Name.ColumnName("mean(${column.name})")), "DOUBLE")
        val results = Recordset(arrayOf(resultsColumn))
        if (parent.rowCount > 0) {
            var sum = 0.0
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
            results.addRowUnsafe(arrayOf(DoubleValue(sum / parent.rowCount)))
            return results
        } else {
            results.addRowUnsafe(arrayOf(DoubleValue(0.0)))
            return results
        }
    }
}