package org.vitrivr.cottontail.dbms.execution.operators.system

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.dbms.execution.locking.LockManager
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.general.DBO
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.ColumnSets

/**
 * An [Operator.SourceOperator] used during query execution. Used to list all locks.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class ListLocksOperator(val manager: LockManager<DBO>, override val context: QueryContext) : Operator.SourceOperator() {
    override val columns: List<ColumnDef<*>> = ColumnSets.DDL_LOCKS_COLUMNS
    override fun toFlow(): Flow<Tuple> = flow {
        var row = 0L
        val columns = this@ListLocksOperator.columns.toTypedArray()
        for (lock in  this@ListLocksOperator.manager.allLocks()) {
            val owners = lock.second.getOwners().map { it.transactionId }
            val values = arrayOf<Value?>(
                StringValue(lock.first.name.toString()),
                StringValue(lock.second.getMode().toString()),
                IntValue(owners.size),
                StringValue(owners.joinToString(", "))
            )
            emit(StandaloneTuple(row++, columns, values))
        }
    }
}