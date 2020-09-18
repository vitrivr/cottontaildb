package org.vitrivr.cottontail.execution.operators.projection

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.column.*
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.exceptions.OperatorExecutionException
import org.vitrivr.cottontail.execution.exceptions.OperatorSetupException
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.basics.OperatorStatus
import org.vitrivr.cottontail.execution.operators.basics.PipelineBreaker
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.*
import kotlin.math.max

/**
 * A [PipelineBreaker] used during query execution. It tracks the maximum value it has encountered
 * so far and returns it as a [Record]. The [MaxProjectionOperator] retains the type of the incoming records.
 *
 * Only produces a single [Record].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class MaxProjectionOperator(parent: Operator, context: ExecutionEngine.ExecutionContext, val column: ColumnDef<*>) : PipelineBreaker(parent, context) {

    /** Columns produced by [MaxProjectionOperator]. */
    override val columns: Array<ColumnDef<*>> = when (this.column.type) {
        ByteColumnType -> arrayOf(ColumnDef.withAttributes(parent.columns.first().name.entity()?.column("max(${column.name})")
                ?: Name.ColumnName("max(${column.name})"), "BYTE"))
        ShortColumnType -> arrayOf(ColumnDef.withAttributes(parent.columns.first().name.entity()?.column("max(${column.name})")
                ?: Name.ColumnName("max(${column.name})"), "SHORT"))
        IntColumnType -> arrayOf(ColumnDef.withAttributes(parent.columns.first().name.entity()?.column("max(${column.name})")
                ?: Name.ColumnName("max(${column.name})"), "INT"))
        LongColumnType -> arrayOf(ColumnDef.withAttributes(parent.columns.first().name.entity()?.column("max(${column.name})")
                ?: Name.ColumnName("max(${column.name})"), "LONG"))
        FloatColumnType -> arrayOf(ColumnDef.withAttributes(parent.columns.first().name.entity()?.column("max(${column.name})")
                ?: Name.ColumnName("max(${column.name})"), "FLOAT"))
        DoubleColumnType -> arrayOf(ColumnDef.withAttributes(parent.columns.first().name.entity()?.column("max(${column.name})")
                ?: Name.ColumnName("max(${column.name})"), "DOUBLE"))
        else -> throw OperatorSetupException(this, "The provided column $column type cannot be used for a MAX projection. ")
    }

    override fun prepareOpen() { /*NoOp */
    }

    override fun prepareClose() { /*NoOp */
    }

    /**
     * Converts this [CountProjectionOperator] to a [Flow] and returns it.
     *
     * @param scope The [CoroutineScope] used for execution
     * @return [Flow] representing this [CountProjectionOperator]
     * @throws IllegalStateException If this [Operator.status] is not [OperatorStatus.OPEN]
     */
    override fun toFlow(scope: CoroutineScope): Flow<Record> {
        check(this.status == OperatorStatus.OPEN) { "Cannot convert operator $this to flow because it is in state ${this.status}." }

        val parentFlow = this.parent.toFlow(scope)
        return flow {
            var max = Double.MIN_VALUE
            parentFlow.collect {
                val value = it[this@MaxProjectionOperator.column]
                when (value) {
                    is ByteValue -> max = max(max, value.value.toDouble())
                    is ShortValue -> max = max(max, value.value.toDouble())
                    is IntValue -> max = max(max, value.value.toDouble())
                    is LongValue -> max = max(max, value.value.toDouble())
                    is FloatValue -> max = max(max, value.value.toDouble())
                    is DoubleValue -> max = max(max, value.value)
                    else -> throw OperatorExecutionException(this@MaxProjectionOperator, "The provided column $column cannot be used for a MAX projection. ")
                }
            }
            when (this@MaxProjectionOperator.column.type) {
                ByteColumnType -> emit(StandaloneRecord(0L, this@MaxProjectionOperator.columns, arrayOf(ByteValue(max))))
                ShortColumnType -> emit(StandaloneRecord(0L, this@MaxProjectionOperator.columns, arrayOf(ShortValue(max))))
                IntColumnType -> emit(StandaloneRecord(0L, this@MaxProjectionOperator.columns, arrayOf(IntValue(max))))
                LongColumnType -> emit(StandaloneRecord(0L, this@MaxProjectionOperator.columns, arrayOf(LongValue(max))))
                FloatColumnType -> emit(StandaloneRecord(0L, this@MaxProjectionOperator.columns, arrayOf(FloatValue(max))))
                DoubleColumnType -> emit(StandaloneRecord(0L, this@MaxProjectionOperator.columns, arrayOf(DoubleValue(max))))
                else -> throw OperatorExecutionException(this@MaxProjectionOperator, "The provided column $column cannot be used for a MAX projection. ")
            }
        }
    }
}