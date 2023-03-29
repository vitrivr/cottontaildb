package org.vitrivr.cottontail.dbms.execution.operators.projection.merge

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.core.values.types.RealValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.exceptions.ExecutionException
import org.vitrivr.cottontail.dbms.execution.exceptions.OperatorSetupException
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.projection.Projection

/**
 * A [Operator.PipelineOperator] used during query execution. It tracks the MIN (minimum) it has
 * encountered so far for each column and returns it as a [Record]. Can collect strands of execution in parallel.
 *
 * The [MergeMaxProjectionOperator] retains the [Types] of the incoming entries! Only produces a
 * single [Record]. Acts as pipeline breaker.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class MergeMinProjectionOperator(parents: List<Operator>, fields: List<Name.ColumnName>, override val context: QueryContext): Operator.MergingPipelineOperator(parents) {

    /** [MergeMinProjectionOperator] does act as a pipeline breaker. */
    override val breaker: Boolean = true

    /** Columns producedthis@MergeMinProjectionOperator]. */
    override val columns: List<ColumnDef<*>> = this.parents.first().columns.mapNotNull { c ->
        val match = fields.find { f -> f.matches(c.name) }
        if (match != null) {
            if (c.type !is Types.Numeric<*>) throw OperatorSetupException(this, "The provided column $match cannot be used for a ${Projection.SUM} projection because it has the wrong type.")
            ColumnDef(c.name, Types.Double)
        } else {
            null
        }
    }

    /** Parent [ColumnDef] to access and aggregate. */
    private val parentColumns = this.parents.first().columns.filter { c -> fields.any { f -> f.matches(c.name) } }

    /**
     * Converts tthis@MergeMinProjectionOperator] to a [Flow] and returns it.
     *
     * @return [Flow] representing tthis@MergeMinProjectionOperator]
     */
    @Suppress("UNCHECKED_CAST")
    override fun toFlow(): Flow<Record> = channelFlow {
        /* Prepare a global heap selection. */
        val incoming = this@MergeMinProjectionOperator.parents
        val columns = this@MergeMinProjectionOperator.columns.toTypedArray()

        /* Prepare holder of type. */
        val globalMin: Array<RealValue<*>> = this@MergeMinProjectionOperator.parentColumns.map {
            when (it.type) {
                is Types.Byte -> ByteValue.ZERO
                is Types.Short -> ShortValue.ZERO
                is Types.Int -> IntValue.ZERO
                is Types.Long -> LongValue.ZERO
                is Types.Float -> FloatValue.ZERO
                is Types.Double -> DoubleValue.ZERO
                else -> throw ExecutionException.OperatorExecutionException(this@MergeMinProjectionOperator, "The provided column $it cannot be used for a ${Projection.SUM} projection. ")
            }
        }.toTypedArray()

        /*
         * Collect incoming flows into a local HeapSelection object (one per flow to avoid contention).
         *
         * For pre-sorted and pre-limited input, the HeapSelection should incur only minimal overhead because
         * sorting only kicks in if k entries have been added.
         */
        val mutex = Mutex()
        val jobs = incoming.map { op ->
            launch {
                val localMin: Array<RealValue<*>> = this@MergeMinProjectionOperator.parentColumns.map {
                    when (it.type) {
                        is Types.Byte -> ByteValue.ZERO
                        is Types.Short -> ShortValue.ZERO
                        is Types.Int -> IntValue.ZERO
                        is Types.Long -> LongValue.ZERO
                        is Types.Float -> FloatValue.ZERO
                        is Types.Double -> DoubleValue.ZERO
                        else -> throw ExecutionException.OperatorExecutionException(this@MergeMinProjectionOperator, "The provided column $it cannot be used for a ${Projection.SUM} projection. ")
                    }
                }.toTypedArray()
                op.toFlow().collect { r ->
                    for ((i,c) in this@MergeMinProjectionOperator.parentColumns.withIndex()) {
                        localMin[i] = RealValue.min(localMin[i], r[c] as RealValue<*>)
                    }
                }
                mutex.withLock {
                    for (i in this@MergeMinProjectionOperator.parentColumns.indices) {
                        globalMin[i] = RealValue.min(globalMin[i], localMin[i])
                    }
                }
            }
        }
        jobs.forEach { it.join() } /* Wait for jobs to complete. */

        /** Emit record. */
        send(StandaloneRecord(0L, columns, globalMin as Array<Value?>))
    }
}