package org.vitrivr.cottontail.execution.tasks.entity.projection

import com.github.dexecutor.core.task.Task
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.query
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionTask
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.model.values.*
import java.lang.Double.min

/**
 * A [Task] used during query execution. It takes a single [Entity] and determines the minimum value of a specific [ColumnDef]. It thereby creates a 1x1 [Recordset].
 *
 * @author Ralph Gasser
 * @version 1.0.2
 */
class EntityMinProjectionTask(val entity: Entity, val column: ColumnDef<*>, val alias: String? = null) : ExecutionTask("EntityMinProjectionTask[${entity.name}]") {

    /**
     * Executes this [EntityExistsProjectionTask]
     */
    override fun execute(): Recordset {
        assertNullaryInput()

        val resultsColumn = ColumnDef.withAttributes(this.entity.name.column("min(${this.column.name})"), "DOUBLE")

        return this.entity.Tx(true, columns = arrayOf(this.column)).query { tx ->
            var min = Double.MAX_VALUE
            val recordset = Recordset(arrayOf(resultsColumn), capacity = 1)
            tx.forEach {
                when (val value = it[column]) {
                    is ByteValue -> min = min(min, value.value.toDouble())
                    is ShortValue -> min = min(min, value.value.toDouble())
                    is IntValue -> min = min(min, value.value.toDouble())
                    is LongValue -> min = min(min, value.value.toDouble())
                    is FloatValue -> min = min(min, value.value.toDouble())
                    is DoubleValue -> min = min(min, value.value)
                    else -> {}
                }
            }
            recordset.addRowUnsafe(arrayOf(DoubleValue(min)))
            recordset
        }!!
    }
}