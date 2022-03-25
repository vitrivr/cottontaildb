package org.vitrivr.cottontail.dbms.execution.operators.system

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionManager

/**
 * An [Operator.SourceOperator] used during query execution. Used to list all ongoing transactions.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class ListTransactionsOperator(val manager: TransactionManager) : Operator.SourceOperator() {
    companion object {
        val COLUMNS: List<ColumnDef<*>> = listOf(
            ColumnDef(Name.ColumnName("txId"), Types.Long, false),
            ColumnDef(Name.ColumnName("type"), Types.String, false),
            ColumnDef(Name.ColumnName("state"), Types.String, false),
            ColumnDef(Name.ColumnName("lock_count"), Types.Int, false),
            ColumnDef(Name.ColumnName("tx_count"), Types.Int, false),
            ColumnDef(Name.ColumnName("created"), Types.Date, false),
            ColumnDef(Name.ColumnName("ended"), Types.Date, true),
            ColumnDef(Name.ColumnName("duration[s]"), Types.Double, true)

        )
    }

    override val columns: List<ColumnDef<*>> = COLUMNS

    override fun toFlow(context: TransactionContext): Flow<Record> {
        val values = Array<Value?>(this@ListTransactionsOperator.columns.size) { null }
        val columns = this.columns.toTypedArray()
        return flow {
            var row = 0L
            this@ListTransactionsOperator.manager.transactionHistory.forEach {
                values[0] = LongValue(it.txId)
                values[1] = StringValue(it.type.toString())
                values[2] = StringValue(it.state.toString())
                values[3] = IntValue(it.numberOfLocks)
                values[4] = IntValue(it.numberOfTxs)
                values[5] = DateValue(it.created)
                values[6] = if (it.ended != null) {
                    DateValue(it.ended!!)
                } else {
                    null
                }
                values[7] = if (it.ended != null) {
                    DoubleValue((it.ended!! - it.created) / 1000.0)
                } else {
                    DoubleValue((System.currentTimeMillis() - it.created) / 1000.0)

                }
                emit(StandaloneRecord(row++, columns, values))
            }
        }
    }
}