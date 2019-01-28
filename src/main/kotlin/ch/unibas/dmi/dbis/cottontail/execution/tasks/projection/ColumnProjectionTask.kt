package ch.unibas.dmi.dbis.cottontail.execution.tasks.projection

import ch.unibas.dmi.dbis.cottontail.database.column.ColumnDef
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.begin
import ch.unibas.dmi.dbis.cottontail.execution.tasks.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.model.basics.Recordset

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
internal class ColumnProjectionTask (
        private val entity: Entity,
        private vararg val columns: ColumnDef<*>
): ExecutionTask("Projection[${entity.fqn}]${columns.joinToString(separator = "") { "[${it.name}]" }}") {

    /**
     *
     */
    override fun execute(): Recordset {
        assertUnaryInput()

        /* Get records from parent task. */
        val parent = this.first() ?: throw TaskExecutionException("Projection could not be executed because parent task has failed.")

        /* Create new Recordset with new columns. */
        val recordset = Recordset(*parent.columns, *this.columns)
        this.entity.Tx(true).begin {tx ->
            parent.forEach {rec ->
                if (rec.tupleId != null) {
                    recordset.addRow(rec.tupleId!!, *rec.values, *tx.read(rec.tupleId!!, *this.columns).values)
                }
            }
            true
        }
        return recordset
    }
}