package org.vitrivr.cottontail.execution.tasks.recordset.projection

import com.github.dexecutor.core.task.Task
import com.github.dexecutor.core.task.TaskExecutionException
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionTask
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.values.*
import java.lang.Double.min

/**
 * A [Task] used during query execution. It takes a single [Recordset] and determines the minimum value of a specific [ColumnDef]. It thereby creates a 1x1 [Recordset].
 *
 * @author Ralph Gasser
 * @version 1.1
 */
class RecordsetMinProjectionTask(val columns: Array<ColumnDef<*>>, val fields: Map<Name.ColumnName, Name.ColumnName?>) : ExecutionTask("RecordsetMinProjectionTask") {

    /**
     * Executes this [RecordsetCountProjectionTask]
     */
    override fun execute(): Recordset {
        assertUnaryInput()

        /* Get records from parent task. */
        val parent = this.first()
                ?: throw TaskExecutionException("MIN projection could not be executed because parent task has failed.")

        /* Calculate min(). */
        val column = this.columns.first()
        val resultsColumn = ColumnDef.withAttributes(this.fields[column.name]
                ?: (column.name.entity()?.column("min(${column.name})") ?: Name.ColumnName("min(${column.name})")), "DOUBLE")
        var min = Double.MAX_VALUE
        val results = Recordset(arrayOf(resultsColumn))
        parent.forEach {
            when (val value = it[column]) {
                is ByteValue -> min = min(min, value.value.toDouble())
                is ShortValue -> min = min(min, value.value.toDouble())
                is IntValue -> min = min(min, value.value.toDouble())
                is LongValue -> min = min(min, value.value.toDouble())
                is FloatValue -> min = min(min, value.value.toDouble())
                is DoubleValue -> min = min(min, value.value)
                else -> {}
            }
        }
        results.addRowUnsafe(arrayOf(DoubleValue(min)))
        return results
    }
}