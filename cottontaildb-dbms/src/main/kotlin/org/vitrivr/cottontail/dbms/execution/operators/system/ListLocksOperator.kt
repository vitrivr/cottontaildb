package org.vitrivr.cottontail.dbms.execution.operators.system

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.dbms.general.DBO
import org.vitrivr.cottontail.dbms.locking.LockManager
import org.vitrivr.cottontail.dbms.execution.TransactionContext
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.types.Value

/**
 * An [Operator.SourceOperator] used during query execution. Used to list all locks.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class ListLocksOperator(val manager: LockManager<DBO>) : Operator.SourceOperator() {
    companion object {
        val COLUMNS: List<ColumnDef<*>> = listOf(
            ColumnDef(Name.ColumnName("dbo"), Types.String, false),
            ColumnDef(Name.ColumnName("mode"), Types.String, false),
            ColumnDef(Name.ColumnName("owner_count"), Types.Int, false),
            ColumnDef(Name.ColumnName("owners"), Types.String, false)
        )
    }

    override val columns: List<ColumnDef<*>> = COLUMNS

    override fun toFlow(context: org.vitrivr.cottontail.dbms.execution.TransactionContext): Flow<Record> {
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