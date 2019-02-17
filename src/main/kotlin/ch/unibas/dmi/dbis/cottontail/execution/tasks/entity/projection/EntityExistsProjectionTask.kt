package ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.projection

import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.query
import ch.unibas.dmi.dbis.cottontail.execution.tasks.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.basics.Recordset

import com.github.dexecutor.core.task.Task

/**
 * A [Task] used during query execution. It takes a single [Entity] and checks if it contains any entries.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class EntityExistsProjectionTask(val entity: Entity, val alias: String? = null): ExecutionTask("EntityExistsProjectionTask[${entity.name}]") {
    override fun execute(): Recordset {
        assertNullaryInput()

        val column = arrayOf(ColumnDef.withAttributes(alias ?: "exists(*)", "BOOLEAN"))
        return this.entity.Tx(true).query {
            val recordset = Recordset(column)
            recordset.addRow(it.count() > 0)
            recordset
        } ?: Recordset(column)
    }
}