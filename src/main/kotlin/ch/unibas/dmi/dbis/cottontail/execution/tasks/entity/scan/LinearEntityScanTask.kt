package ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.scan

import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.query
import ch.unibas.dmi.dbis.cottontail.execution.tasks.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.basics.Recordset
import com.github.dexecutor.core.task.Task

/**
 * A [Task] that executes a full table scan on a defined [Entity] and returns all the defined values.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class LinearEntityScanTask(private val entity: Entity, private val columns: Array<ColumnDef<*>>) : ExecutionTask("LinearEntityScanTask[${entity.fqn}][${columns.map { it.name }.joinToString(",")}]") {
    override fun execute(): Recordset = this.entity.Tx(true).query {
        it.readAll(this.columns)
    } ?: Recordset(columns)
}