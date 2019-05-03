package ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.projection

import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.query
import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.model.values.LongValue
import com.github.dexecutor.core.task.Task

/**
 * A [Task] used during query execution. It takes a single [Entity] as input, counts the number of of rows and returns it as [Recordset].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class EntityCountProjectionTask (val entity: Entity): ExecutionTask("EntityCountProjectionTask[${entity.fqn}") {

    /** The cost of this [EntityCountProjectionTask] is constant */
    override val cost = 0.25f

    /**
     * Executes this [EntityCountProjectionTask]
     */
    override fun execute(): Recordset {
        assertNullaryInput()

        val column = arrayOf(ColumnDef.withAttributes("${entity.fqn}.count(*)", "INTEGER"))
        return this.entity.Tx(true).query {
            val recordset = Recordset(column)
            recordset.addRowUnsafe(arrayOf(LongValue(it.count())))
            recordset
        } ?: Recordset(column)
    }
}