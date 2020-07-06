package org.vitrivr.cottontail.execution.tasks.entity.fetch

import com.github.dexecutor.core.task.Task
import com.github.dexecutor.core.task.TaskExecutionException
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.begin
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionTask
import org.vitrivr.cottontail.execution.tasks.recordset.projection.RecordsetCountProjectionTask
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A [Task] used during query execution. It takes a [Recordset] as input, fetches the desired [Column][org.vitrivr.cottontail.database.column.Column]s
 * from the specified [Entity] and appends them to the original [Recordset].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class EntityFetchColumnsTask(val entity: Entity, val columns: Array<ColumnDef<*>>) : ExecutionTask("EntityFetchColumnsTask[${entity.name}") {
    /**
     * Executes this [RecordsetCountProjectionTask]
     */
    override fun execute(): Recordset {
        assertUnaryInput()

        /* Get records from parent task. */
        val parent = this.first()
                ?: throw TaskExecutionException("Projection could not be executed because parent task has failed.")

        /* Determine the columns to fetch and all columns. */
        val all = (parent.columns + columns).distinct().toTypedArray()
        val fetch = this.columns.filter { !parent.columns.contains(it) }.toTypedArray()

        /* Prepare new Recordset and fill it with data. */
        if (fetch.isNotEmpty()) {
            val recordset = Recordset(all, capacity = parent.rowCount)
            this.entity.Tx(readonly = true, columns = fetch).begin { tx ->
                parent.forEach { rec ->
                    val read = tx.read(rec.tupleId)
                    val values: Array<Value?> = all.map {
                        if (read.has(it)) {
                            read[it]
                        } else {
                            rec[it]
                        }
                    }.toTypedArray()
                    recordset.addRowUnsafe(rec.tupleId, values)
                }
                true
            }

            return recordset
        } else {
            return parent
        }
    }
}