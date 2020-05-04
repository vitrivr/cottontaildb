package org.vitrivr.cottontail.execution.tasks.entity.projection

import com.github.dexecutor.core.task.Task
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.query
import org.vitrivr.cottontail.execution.cost.Costs
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionTask
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.values.*
import org.vitrivr.cottontail.utilities.name.Name
import kotlin.math.max

/**
 * A [Task] used during query execution. It takes a single [Entity] and determines the maximum value of a specific [ColumnDef]. It thereby creates a 1x1 [Recordset].
 *
 * @author Ralph Gasser
 * @version 1.0.2
 */
class EntityMaxProjectionTask(val entity: Entity, val column: ColumnDef<*>, val alias: String? = null) : ExecutionTask("EntityMaxProjectionTask[${entity.name}]") {

    /** The cost of this [EntityExistsProjectionTask] is constant */
    override val cost = this.entity.statistics.rows * Costs.DISK_ACCESS_READ

    /**
     * Executes this [EntityExistsProjectionTask]
     */
    override fun execute(): Recordset {
        assertNullaryInput()

        val resultsColumn = ColumnDef.withAttributes(Name(this.alias
                ?: "${entity.fqn}.max(${column.name})"), "DOUBLE")

        return this.entity.Tx(true, columns = arrayOf(this.column)).query {
            var max = Double.MIN_VALUE
            val recordset = Recordset(arrayOf(resultsColumn), capacity = 1)
            it.forEach {
                when (val value = it[column]) {
                    is ByteValue -> max = max(max, value.value.toDouble())
                    is ShortValue -> max = max(max, value.value.toDouble())
                    is IntValue -> max = max(max, value.value.toDouble())
                    is LongValue -> max = max(max, value.value.toDouble())
                    is FloatValue -> max = max(max, value.value.toDouble())
                    is DoubleValue -> max = max(max, value.value)
                    else -> {}
                }
            }
            recordset.addRowUnsafe(arrayOf(DoubleValue(max)))
            recordset
        }!!
    }
}