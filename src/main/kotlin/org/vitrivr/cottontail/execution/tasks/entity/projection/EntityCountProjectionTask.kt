package org.vitrivr.cottontail.execution.tasks.entity.projection

import com.github.dexecutor.core.task.Task
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.query
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionTask
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.values.LongValue
import org.vitrivr.cottontail.utilities.name.Name

/**
 * A [Task] used during query execution. It takes a single [Entity] as input, counts the number of rows. It thereby creates a 1x1 [Recordset].
 *
 * @author Ralph Gasser
 * @version 1.0.3
 */
class EntityCountProjectionTask(val entity: Entity, val alias: String? = null) : ExecutionTask("EntityCountProjectionTask[${entity.fqn}") {

    /**
     * Executes this [EntityCountProjectionTask]
     */
    override fun execute(): Recordset {
        assertNullaryInput()

        val column = arrayOf(ColumnDef.withAttributes(Name(this.alias
                ?: "${entity.fqn}.count()"), "LONG"))
        return this.entity.Tx(true).query {
            val recordset = Recordset(column, capacity = 1)
            recordset.addRowUnsafe(arrayOf(LongValue(it.count())))
            recordset
        } ?: Recordset(column)
    }
}