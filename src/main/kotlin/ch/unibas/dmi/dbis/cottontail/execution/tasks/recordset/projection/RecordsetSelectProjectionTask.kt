package ch.unibas.dmi.dbis.cottontail.execution.tasks.recordset.projection

import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.begin
import ch.unibas.dmi.dbis.cottontail.execution.tasks.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset

import com.github.dexecutor.core.task.Task
import com.github.dexecutor.core.task.TaskExecutionException

/**
 * A [Task] used during query execution. It takes a single [Recordset] as input and fetches the specified [Column] values from the
 * specified [Entity] in order to addRow these values to the resulting [Recordset].
 *
 * A [Recordset] that undergoes this task will grow in terms of columns it contains. However, it should not affect the number of rows.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class RecordsetSelectProjectionTask (
    private val entity: Entity,
    private val columns: Array<ColumnDef<*>>
): ExecutionTask("RecordsetSelectProjectionTask[${entity.fqn}]${columns.joinToString(separator = "") { "[${it.name}]" }}") {

    /**
     *
     */
    override fun execute(): Recordset {
        assertUnaryInput()

        /* Get records from parent task. */
        val parent = this.first() ?: throw TaskExecutionException("Projection could not be executed because parent task has failed.")

        /* Create new Recordset with new columns. */
        val recordset = Recordset(parent.columns + this.columns)
        this.entity.Tx(readonly = true, columns = columns).begin {tx ->
            parent.forEach {rec ->
                recordset.addRow(rec.tupleId, rec.values + tx.read(rec.tupleId).values)
            }
            true
        }
        return recordset
    }
}