package org.vitrivr.cottontail.dbms.execution.operators.management

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
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
 * An [Operator.PipelineOperator] used during query execution. Updates all entries in an [Entity]
 * that it receives with the provided [Value].
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class UpdateOperator(parent: Operator, val entity: EntityTx, val values: List<Pair<ColumnDef<*>, Value?>>) : Operator.PipelineOperator(parent) {

    companion object {
        /** The columns produced by the [UpdateOperator]. */
        val COLUMNS: List<ColumnDef<*>> = listOf(
            ColumnDef(Name.ColumnName("updated"), Types.Long, false),
            ColumnDef(Name.ColumnName("duration_ms"), Types.Double, false)
        )
    }

    /** Columns produced by [UpdateOperator]. */
    override val columns: List<ColumnDef<*>> = COLUMNS

    /** [UpdateOperator] does not act as a pipeline breaker. */
    override val breaker: Boolean = false

    /**
     * Converts this [UpdateOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [UpdateOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        var updated = 0L
        val parent = this.parent.toFlow(context)
        val c = this.values.map { it.first }.toTypedArray()
        val v = this.values.map { it.second }.toTypedArray()
        return flow {
            val start = System.currentTimeMillis()
            parent.collect { record ->
                this@UpdateOperator.entity.update(StandaloneRecord(record.tupleId, c, v)) /* Safe, cause tuple IDs are retained for simple queries. */
                updated += 1
            }
            emit(StandaloneRecord(0L, this@UpdateOperator.columns.toTypedArray(), arrayOf(LongValue(updated), DoubleValue(System.currentTimeMillis() - start))))
        }
    }
}