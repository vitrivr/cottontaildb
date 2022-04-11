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
import kotlin.math.max

/**
 * A [Operator.PipelineOperator] used during query execution. It tracks the MAX (maximum) it has
 * encountered so far for each column and returns it as a [Record].
 *
 * The [MaxProjectionOperator] retains the [Types] of the incoming entries! Only produces a
 * single [Record]. Acts as pipeline breaker.
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
class MaxProjectionOperator(parent: Operator, fields: List<Name.ColumnName>) : Operator.PipelineOperator(parent) {

    /** [MaxProjectionOperator] does act as a pipeline breaker. */
    override val breaker: Boolean = true

    /** Columns produced by [MaxProjectionOperator]. */
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
     * Converts this [CountProjectionOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [CountProjectionOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val parentFlow = this.parent.toFlow(context)
        val columns = this.columns.toTypedArray()
        return flow {
            /* Prepare holder of type double, which can hold all types of values and collect incoming flow */
            val max = this@MaxProjectionOperator.parentColumns.map { Double.MIN_VALUE }.toTypedArray()
            parentFlow.onEach {
                this@MaxProjectionOperator.parentColumns.forEachIndexed { i, c ->
                    max[i] = when (val value = it[c]) {
                        is ByteValue -> max(max[i], value.value.toDouble())
                        is ShortValue -> max(max[i], value.value.toDouble())
                        is IntValue -> max(max[i], value.value.toDouble())
                        is LongValue -> max(max[i], value.value.toDouble())
                        is FloatValue -> max(max[i], value.value.toDouble())
                        is DoubleValue -> max(max[i], value.value)
                        null -> max[i]
                        else -> throw ExecutionException.OperatorExecutionException(this@MaxProjectionOperator, "The provided column $c cannot be used for a MAX projection. ")
                    }
                }
            }.collect()

            /* Convert to original value type. */
            val results = Array<Value?>(max.size) {
                val column = this@MaxProjectionOperator.parentColumns[it]
                when (column.type) {
                    Types.Byte -> ByteValue(max[it])
                    Types.Short -> ShortValue(max[it])
                    Types.Int -> IntValue(max[it])
                    Types.Long -> LongValue(max[it])
                    Types.Float -> FloatValue(max[it])
                    Types.Double -> DoubleValue(max[it])
                    else -> throw ExecutionException.OperatorExecutionException(this@MaxProjectionOperator, "The provided column $column cannot be used for a MAX projection.")
                }
            }

            /** Emit record. */
            emit(StandaloneRecord(0L, columns, results))
        }
    }
}