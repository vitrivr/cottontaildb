package org.vitrivr.cottontail.dbms.execution.operators.projection

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
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
 * An [Operator.PipelineOperator] used during query execution. It tracks the MIN (minimum) value it
 * has encountered so far  and returns it as a [Tuple]. The [MinProjectionOperator] retains the type
 * of the incoming records.
 *
 * Only produces a single [Tuple]. Acts as pipeline breaker.
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
class MinProjectionOperator(parent: Operator, fields: List<Binding.Column>, override val context: QueryContext) : Operator.PipelineOperator(parent) {

    /** [MinProjectionOperator] does act as a pipeline breaker. */
    override val breaker: Boolean = true

    /** Columns produced by [MinProjectionOperator]. */
    override val columns: List<ColumnDef<*>> = fields.map {
        require(it.type is Types.Numeric) { "Projection of type ${Projection.SUM} can only be applied to numeric columns, which $fields isn't." }
        it.column
    }

    /**
     * Converts this [MinProjectionOperator] to a [Flow] and returns it.
     *
     * @return [Flow] representing this [MinProjectionOperator]
     */
    @Suppress("UNCHECKED_CAST")
    override fun toFlow(): Flow<Tuple> = flow {
        val incoming = this@MinProjectionOperator.parent.toFlow()
        val columns = this@MinProjectionOperator.columns.toTypedArray()

        /* Prepare holder of type double, which can hold all types of values and collect incoming flow */
        val results: Array<RealValue<*>> = this@MinProjectionOperator.columns.map {
            when(it.type) {
                is Types.Byte -> ByteValue.MAX_VALUE
                is Types.Short -> ShortValue.MAX_VALUE
                is Types.Int -> IntValue.MAX_VALUE
                is Types.Long -> LongValue.MAX_VALUE
                is Types.Float -> FloatValue.MAX_VALUE
                is Types.Double -> DoubleValue.MAX_VALUE
                else -> throw IllegalArgumentException("Column $it is not supported by the MinProjectionOperator. This is a programmer's error!")
            }
        }.toTypedArray()
        incoming.collect { r ->
            for ((i,c) in this@MinProjectionOperator.columns.withIndex()) {
                results[i] = RealValue.min(results[i], r[c] as RealValue<*>)
            }
        }

        /** Emit record. */
        emit(StandaloneTuple(0L, columns, results as Array<Value?>))
    }
}