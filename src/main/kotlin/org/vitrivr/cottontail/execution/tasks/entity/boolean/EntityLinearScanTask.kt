package org.vitrivr.cottontail.execution.tasks.entity.boolean

import com.github.dexecutor.core.task.Task
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.query
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionTask
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.recordset.Recordset

/**
 * A [Task] that executes a full table boolean on a defined [Entity] and returns all the defined values.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
class EntityLinearScanTask(private val entity: Entity, private val columns: Array<ColumnDef<*>>) : ExecutionTask("EntityLinearScanTask[${entity.fqn}][${columns.map { it.name }.joinToString(",")}]") {

    /** The cost of this [EntityLinearScanTask] is constant */
    override val cost = (entity.statistics.columns * columns.size).toFloat()

    /**
     * Executes this [EntityLinearScanTask]
     */
    override fun execute(): Recordset = this.entity.Tx(readonly = true, columns = this.columns).query {
        it.readAll()
    } ?: Recordset(this.columns, capacity = 0)
}