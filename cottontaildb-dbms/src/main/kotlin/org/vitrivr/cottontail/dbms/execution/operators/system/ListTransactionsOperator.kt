package org.vitrivr.cottontail.dbms.execution.operators.system

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionManager
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.ColumnSets

/**
 * An [Operator.SourceOperator] used during query execution. Used to list all ongoing transactions.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class ListTransactionsOperator(override val context: QueryContext) : Operator.SourceOperator() {
    override val columns: List<ColumnDef<*>>
        get() = ColumnSets.DDL_TRANSACTIONS_COLUMNS

    /** Reference to [TransactionManager] instance. */
    private val manager:     TransactionManager
        get() = this.context.transaction.manager

    override fun toFlow(): Flow<Tuple> = flow {
        val columns = this@ListTransactionsOperator.columns.toTypedArray()
        var row = 0L
        this@ListTransactionsOperator.manager.history().forEach {
            val values = arrayOf<Value?>(
                LongValue(it.transactionId),
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
            emit(StandaloneTuple(row++, columns, values))
        }
    }
}