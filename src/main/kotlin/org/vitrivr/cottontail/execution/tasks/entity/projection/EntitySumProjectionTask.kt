package org.vitrivr.cottontail.execution.tasks.entity.projection

import com.github.dexecutor.core.task.Task
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.query
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionTask
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.values.*

/**
 * A [Task] used during query execution. It takes a single [Entity] and determines the mean value of a specific [ColumnDef]. It thereby creates a 1x1 [Recordset].
 *
 * @author Ralph Gasser
 * @version 1.0.2
 */
class EntitySumProjectionTask(val entity: Entity, val column: ColumnDef<*>, val alias: String? = null) : ExecutionTask("EntitySumProjectionTask[${entity.name}]") {

    /**
     * Executes this [EntityExistsProjectionTask]
     */
    override fun execute(): Recordset {
        assertNullaryInput()

        val resultsColumn = ColumnDef.withAttributes(this.entity.name.column("sum(${this.column.name})"), "DOUBLE")

        return this.entity.Tx(true, columns = arrayOf(this.column)).query { tx ->
            var sum = 0.0
            val recordset = Recordset(arrayOf(resultsColumn), capacity = 1)
            tx.forEach {
                when (val value = it[column]) {
                    is ByteValue -> sum += value.value
                    is ShortValue -> sum += value.value
                    is IntValue -> sum += value.value
                    is LongValue -> sum += value.value
                    is FloatValue -> sum += value.value
                    is DoubleValue -> sum += value.value
                    else -> {}
                }
            }
            recordset.addRowUnsafe(arrayOf(DoubleValue(sum)))
            recordset
        }!!
    }
}