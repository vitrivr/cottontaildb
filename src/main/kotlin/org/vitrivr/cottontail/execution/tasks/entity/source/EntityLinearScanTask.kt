package org.vitrivr.cottontail.execution.tasks.entity.source

import com.github.dexecutor.core.task.Task

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.query
import org.vitrivr.cottontail.execution.tasks.TaskSetupException
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionTask
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.recordset.Recordset

/**
 * A [Task] that executes a table scan on a defined [Entity] and returns all the values.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
class EntityLinearScanTask(private val entity: Entity, private val columns: Array<ColumnDef<*>>, start: Long? = null, end: Long? = null) : ExecutionTask("EntityLinearScanTask[${columns.map { it.name }.joinToString(",")}]") {

    /** Begin of range that should be scanned by this [EntityLinearScanTask]. */
    private val from = start ?: 1L

    /** End of range that should be scanned by this [EntityLinearScanTask]. */
    private val to = end ?: this.entity.statistics.maxTupleId

    init {
        if (this.from < 1L) throw TaskSetupException(this, "EntityLinearScanTask scan range is invalid (from=${this.from}, to=${this.to}).")
        if (this.from > this.to) throw TaskSetupException(this, "EntityLinearScanTask scan range is invalid (from=${this.from}, to=${this.to}).")
    }

    /**
     * Executes this [EntityLinearScanTask]
     */
    override fun execute(): Recordset = this.entity.Tx(readonly = true, columns = this.columns).query { tx ->
        val recordset = Recordset(this.columns, this.to - this.from)
        tx.forEach(this.from, this.to) {
            recordset.addRow(it)
        }
        recordset
    } ?: Recordset(this.columns, capacity = 0)
}