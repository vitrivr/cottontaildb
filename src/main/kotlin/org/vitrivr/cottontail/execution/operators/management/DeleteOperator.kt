package org.vitrivr.cottontail.execution.operators.management

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.predicates.FilterOperator
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.values.types.Types
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.LongValue
import kotlin.time.ExperimentalTime
import kotlin.time.measureTime

/**
 * An [Operator.MergingPipelineOperator] used during query execution. Deletes all entries in an
 * [Entity] that it receives.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class DeleteOperator(parent: Operator, val entity: EntityTx) : Operator.PipelineOperator(parent) {
    companion object {
        /** The columns produced by the [DeleteOperator]. */
        val COLUMNS: List<ColumnDef<*>> = listOf(
            ColumnDef(Name.ColumnName("deleted"), Types.Long, false),
            ColumnDef(Name.ColumnName("duration_ms"), Types.Double, false)
        )
    }

    /** Columns produced by the [DeleteOperator]. */
    override val columns: List<ColumnDef<*>> = COLUMNS

    /** [DeleteOperator] does not act as a pipeline breaker. */
    override val breaker: Boolean = false

    /**
     * Converts this [FilterOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [FilterOperator]
     */
    @ExperimentalTime
    override fun toFlow(context: TransactionContext): Flow<Record> {
        var deleted = 0L
        val parent = this.parent.toFlow(context)
        val columns = this.columns.toTypedArray()
        return flow {
            val time = measureTime {
                parent.collect {
                    this@DeleteOperator.entity.delete(it.tupleId) /* Safe, cause tuple IDs are retained for simple queries. */
                    deleted += 1
                }
            }
            emit(StandaloneRecord(0L, columns, arrayOf(LongValue(deleted), DoubleValue(time.inWholeMilliseconds))))
        }
    }
}