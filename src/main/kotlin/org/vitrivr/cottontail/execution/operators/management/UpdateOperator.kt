package org.vitrivr.cottontail.execution.operators.management

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.values.types.Types
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.LongValue
import org.vitrivr.cottontail.model.values.types.Value
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

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
    @ExperimentalTime
    override fun toFlow(context: TransactionContext): Flow<Record> {
        var updated = 0L
        val parent = this.parent.toFlow(context)
        val c = this.values.map { it.first }.toTypedArray()
        val v = this.values.map { it.second }.toTypedArray()
        return flow {
            val time = measureTime {
                parent.collect { record ->
                    this@UpdateOperator.entity.update(StandaloneRecord(record.tupleId, c, v)) /* Safe, cause tuple IDs are retained for simple queries. */
                    updated += 1
                }
            }
            emit(StandaloneRecord(0L, this@UpdateOperator.columns.toTypedArray(), arrayOf(LongValue(updated), DoubleValue(time.inWholeMilliseconds))))
        }
    }
}