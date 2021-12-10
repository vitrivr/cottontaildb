package org.vitrivr.cottontail.execution.operators.system

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.general.DBO
import org.vitrivr.cottontail.database.locking.LockManager
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.database.queries.binding.EmptyBindingContext
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.definition.AbstractDataDefinitionOperator
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.IntValue
import org.vitrivr.cottontail.model.values.StringValue
import org.vitrivr.cottontail.model.values.types.Value

/**
 * An [Operator.SourceOperator] used during query execution. Used to list all locks.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class ListLocksOperator(val manager: LockManager<DBO>) : Operator.SourceOperator() {
    companion object {
        val COLUMNS: List<ColumnDef<*>> = listOf(
            ColumnDef(Name.ColumnName("dbo"), Type.String, false),
            ColumnDef(Name.ColumnName("mode"), Type.String, false),
            ColumnDef(Name.ColumnName("owner_count"), Type.Int, false),
            ColumnDef(Name.ColumnName("owners"), Type.String, false)
        )
    }

    /** The [BindingContext] used [AbstractDataDefinitionOperator]. */
    override val binding: BindingContext = EmptyBindingContext

    override val columns: List<ColumnDef<*>> = COLUMNS

    override fun toFlow(context: TransactionContext): Flow<Record> {
        val columns = this.columns.toTypedArray()
        val values = Array<Value?>(this@ListLocksOperator.columns.size) { null }
        return flow {
            var row = 0L
            this@ListLocksOperator.manager.allLocks().forEach { lock ->
                values[0] = StringValue(lock.first.name.toString())
                values[1] = StringValue(lock.second.getMode().toString())
                val owners = lock.second.getOwners().map { it.txId }
                values[2] = IntValue(owners.size)
                values[3] = StringValue(owners.joinToString(", "))
                emit(StandaloneRecord(row++, columns, values))
            }
        }
    }
}