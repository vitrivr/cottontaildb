package org.vitrivr.cottontail.dbms.execution.operators.projection

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onEach
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.RealValue
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.projection.Projection

/**
 * A [Operator.PipelineOperator] used during query execution. It tracks the MAX (maximum) it has
 * encountered so far for each column and returns it as a [Tuple].
 *
 * The [MaxProjectionOperator] retains the [Types] of the incoming entries! Only produces a
 * single [Tuple]. Acts as pipeline breaker.
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
class MaxProjectionOperator(parent: Operator, fields: List<Binding.Column>, override val context: QueryContext) : Operator.PipelineOperator(parent) {

    /** [MaxProjectionOperator] does act as a pipeline breaker. */
    override val breaker: Boolean = true

    /** Columns produced by [MaxProjectionOperator]. */
    override val columns: List<ColumnDef<*>> = fields.map {
        require(it.type is Types.Numeric) { "Projection of type ${Projection.SUM} can only be applied to numeric columns, which $fields isn't." }
        it.column
    }

    /**
     * Converts this [CountProjectionOperator] to a [Flow] and returns it.
     *
     * @return [Flow] representing this [CountProjectionOperator]
     */
    @Suppress("UNCHECKED_CAST")
    override fun toFlow(): Flow<Tuple> = flow {
        val incoming = this@MaxProjectionOperator.parent.toFlow()
        val columns = this@MaxProjectionOperator.columns.toTypedArray()

        /* Prepare holder of type double, which can hold all types of values and collect incoming flow */
        val results: Array<RealValue<*>> = this@MaxProjectionOperator.columns.map {
            when(it.type) {
                is Types.Byte -> ByteValue.MIN_VALUE
                is Types.Short -> ShortValue.MIN_VALUE
                is Types.Int -> IntValue.MIN_VALUE
                is Types.Long -> LongValue.MIN_VALUE
                is Types.Float -> FloatValue.MIN_VALUE
                is Types.Double -> DoubleValue.MIN_VALUE
                else -> throw IllegalArgumentException("Column $it is not supported by t he MaxProjectionOperator. This is a programmer's error!")
            }
        }.toTypedArray()
        incoming.onEach { r ->
            for ((i, c) in this@MaxProjectionOperator.columns.withIndex()) {
                results[i] = RealValue.max(results[i], r[c] as RealValue<*>)
            }
        }.collect()

        /** Emit record. */
        emit(StandaloneTuple(0L, columns, results as Array<Value?>))
    }
}