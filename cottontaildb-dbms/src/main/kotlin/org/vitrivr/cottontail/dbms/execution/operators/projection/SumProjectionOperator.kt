package org.vitrivr.cottontail.dbms.execution.operators.projection

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.NumericValue
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.dbms.exceptions.ExecutionException
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.projection.Projection

/**
 * An [Operator.PipelineOperator] used during query execution. It calculates the SUM of all values it
 * has encountered and returns it as a [Tuple].
 *
 * Only produces a single [Tuple] and converts the projected columns to a [Types.Double] column.
 * Acts as pipeline breaker.
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
class SumProjectionOperator(parent: Operator, fields: List<Binding.Column>, override val context: QueryContext) : Operator.PipelineOperator(parent) {
    /** [SumProjectionOperator] does act as a pipeline breaker. */
    override val breaker: Boolean = true

    /** Columns produced by [SumProjectionOperator]. */
    override val columns: List<ColumnDef<*>> = fields.map {
        require(it.type is Types.Numeric) { "Projection of type ${Projection.SUM} can only be applied to numeric columns, which $fields isn't." }
        it.column
    }

    /**
     * Converts this [SumProjectionOperator] to a [Flow] and returns it.
     *
     * @return [Flow] representing this [SumProjectionOperator]
     */
    override fun toFlow(): Flow<Tuple> = flow {
        val incoming = this@SumProjectionOperator.parent.toFlow()
        val columns = this@SumProjectionOperator.columns.toTypedArray()
        val values: Array<NumericValue<*>> = this@SumProjectionOperator.columns.map {
            when (it.type) {
                is Types.Byte -> ByteValue.ZERO
                is Types.Short -> ShortValue.ZERO
                is Types.Int -> IntValue.ZERO
                is Types.Long -> LongValue.ZERO
                is Types.Float -> FloatValue.ZERO
                is Types.Double -> DoubleValue.ZERO
                is Types.Complex32 -> Complex32Value.ZERO
                is Types.Complex64 -> Complex64Value.ZERO
                else -> throw ExecutionException.OperatorExecutionException(this@SumProjectionOperator, "The provided column $it cannot be used for a ${Projection.SUM} projection. ")
            }
        }.toTypedArray()

        /* Prepare holder of type double. */
        incoming.collect {
            for ((i,c) in this@SumProjectionOperator.columns.withIndex()) {
                values[i] += it[c] as NumericValue<*>
            }
        }

        /** Emit record. */
        emit(StandaloneTuple(0L, columns, values.map { DoubleValue(it) }.toTypedArray()))
    }
}