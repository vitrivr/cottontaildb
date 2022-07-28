package org.vitrivr.cottontail.dbms.execution.operators.projection

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.core.values.types.RealValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.execution.exceptions.OperatorSetupException
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.projection.Projection

/**
 * An [Operator.PipelineOperator] used during query execution. It tracks the MIN (minimum) value it
 * has encountered so far  and returns it as a [Record]. The [MinProjectionOperator] retains the type
 * of the incoming records.
 *
 * Only produces a single [Record]. Acts as pipeline breaker.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class MinProjectionOperator(parent: Operator, fields: List<Name.ColumnName>, override val context: QueryContext) : Operator.PipelineOperator(parent) {

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
     * @return [Flow] representing this [MinProjectionOperator]
     */
    @Suppress("UNCHECKED_CAST")
    override fun toFlow(): Flow<Record> = flow {
        val incoming = this@MinProjectionOperator.parent.toFlow()
        val columns = this@MinProjectionOperator.columns.toTypedArray()

        /* Prepare holder of type double, which can hold all types of values and collect incoming flow */
        val results: Array<RealValue<*>> = this@MinProjectionOperator.parentColumns.map {
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
            for (i in results.indices) {
                results[i] = RealValue.min(results[i], r[columns[i]] as RealValue<*>)
            }
        }

        /** Emit record. */
        emit(StandaloneRecord(0L, columns, results as Array<Value?>))
    }
}