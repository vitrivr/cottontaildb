package org.vitrivr.cottontail.dbms.execution.operators.projection

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onEach
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.exceptions.ExecutionException
import org.vitrivr.cottontail.dbms.execution.exceptions.OperatorSetupException
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.queries.projection.Projection
import kotlin.math.min

/**
 * An [Operator.PipelineOperator] used during query execution. It tracks the MIN (minimum) value it
 * has encountered so far  and returns it as a [Record]. The [MinProjectionOperator] retains the type
 * of the incoming records.
 *
 * Only produces a single [Record]. Acts as pipeline breaker.
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
class MinProjectionOperator(parent: Operator, fields: List<Name.ColumnName>) : Operator.PipelineOperator(parent) {

    /** [MinProjectionOperator] does act as a pipeline breaker. */
    override val breaker: Boolean = true

    /** Columns produced by [MinProjectionOperator]. */
    override val columns: List<ColumnDef<*>> = this.parent.columns.mapNotNull { c ->
        val match = fields.find { f -> f.matches(c.name) }
        if (match != null) {
            if (c.type !is Types.Numeric<*>) throw OperatorSetupException(this, "The provided column $match cannot be used for a ${Projection.MAX} projection because it has the wrong type.")
            c
        } else {
            null
        }
    }

    /** Parent [ColumnDef] to access and aggregate. */
    private val parentColumns = this.parent.columns.filter { c -> fields.any { f -> f.matches(c.name) } }

    /**
     * Converts this [MinProjectionOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [MinProjectionOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val parentFlow = this.parent.toFlow(context)
        val columns = this.columns.toTypedArray()
        return flow {
            /* Prepare holder of type double, which can hold all types of values and collect incoming flow */
            val min = this@MinProjectionOperator.parentColumns.map { Double.MAX_VALUE }.toTypedArray()
            parentFlow.onEach {
                this@MinProjectionOperator.parentColumns.forEachIndexed { i, c ->
                    min[i] = when (val value = it[c]) {
                        is ByteValue -> min(min[i], value.value.toDouble())
                        is ShortValue -> min(min[i], value.value.toDouble())
                        is IntValue -> min(min[i], value.value.toDouble())
                        is LongValue -> min(min[i], value.value.toDouble())
                        is FloatValue -> min(min[i], value.value.toDouble())
                        is DoubleValue -> min(min[i], value.value)
                        null -> min[i]
                        else -> throw ExecutionException.OperatorExecutionException(this@MinProjectionOperator, "The provided column $c cannot be used for a MIN projection. ")
                    }
                }
            }.collect()

            /* Convert to original value type. */
            val results = Array<Value?>(min.size) {
                val column = this@MinProjectionOperator.parentColumns[it]
                when (column.type) {
                    Types.Boolean -> ByteValue(min[it])
                    Types.Short -> ShortValue(min[it])
                    Types.Int -> IntValue(min[it])
                    Types.Long -> LongValue(min[it])
                    Types.Float -> FloatValue(min[it])
                    Types.Double -> DoubleValue(min[it])
                    else -> throw ExecutionException.OperatorExecutionException(this@MinProjectionOperator, "The provided column $column cannot be used for a MIN projection.")
                }
            }

            /** Emit record. */
            emit(StandaloneRecord(0L, columns, results))
        }
    }
}