package org.vitrivr.cottontail.dbms.execution.operators.system

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.DateValue
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionManager
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.ColumnSets

/**
 * An [Operator.SourceOperator] used during query execution. Used to list all ongoing transactions.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class ListTransactionsOperator(val manager: TransactionManager, override val context: QueryContext) : Operator.SourceOperator() {
    override val columns: List<ColumnDef<*>>
        get() = ColumnSets.DDL_TRANSACTIONS_COLUMNS

    override fun toFlow(): Flow<Record> = flow {
        val columns = this@ListTransactionsOperator.columns.toTypedArray()
        var row = 0L
        this@ListTransactionsOperator.manager.transactionHistory.forEach {
            val values = arrayOf<Value?>(
                LongValue(it.txId),
                StringValue(it.type.toString()),
                StringValue(it.state.toString()),
                DateValue(it.created),
                if (it.ended != null) {
                    DateValue(it.ended!!)
                } else {
                    null
                },
                if (it.ended != null) {
                    DoubleValue((it.ended!! - it.created) / 1000.0)
                } else {
                    DoubleValue((System.currentTimeMillis() - it.created) / 1000.0)
                }
            )
            emit(StandaloneRecord(row++, columns, values))
        }
    }
}