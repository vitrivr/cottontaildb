package org.vitrivr.cottontail.dbms.execution.operators.management

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext

/**
 * An [Operator.PipelineOperator] used during query execution. Inserts all incoming entries into an
 * [Entity] that it receives with the provided [Value].
 *
 * @author Ralph Gasser
 * @version 1.5.0
 */
class InsertOperator(groupId: GroupId, val entity: EntityTx, val records: List<Record>) : Operator.SourceOperator(groupId) {
    companion object {
        /** The columns produced by the [InsertOperator]. */
        val COLUMNS: List<ColumnDef<*>> = listOf(
            ColumnDef(Name.ColumnName("tupleId"), Types.Long, false),
            ColumnDef(Name.ColumnName("duration_ms"), Types.Double, false)
        )
    }

    /** Columns produced by [InsertOperator]. */
    override val columns: List<ColumnDef<*>> = COLUMNS + this.entity.listColumns()

    /**
     * Converts this [InsertOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [InsertOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> = flow {
        val columns = this@InsertOperator.columns.toTypedArray()
        for (record in this@InsertOperator.records) {
            val start = System.currentTimeMillis()
            val created = this@InsertOperator.entity.insert(record)
            emit(StandaloneRecord(
                0L,
                columns,
                arrayOf<Value?>(LongValue(created.tupleId), DoubleValue(System.currentTimeMillis() - start)) + Array(record.size) { created[it + 2]}
            ))
        }
    }
}