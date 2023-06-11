package org.vitrivr.cottontail.dbms.execution.operators.projection.merge

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.NumericValue
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.*
import org.vitrivr.cottontail.dbms.exceptions.ExecutionException
import org.vitrivr.cottontail.dbms.execution.exceptions.OperatorSetupException
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.projection.MaxProjectionOperator
import org.vitrivr.cottontail.dbms.execution.operators.projection.MeanProjectionOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.projection.Projection


/**
 * An [Operator.MergingPipelineOperator] used during query execution. It calculates the MEAN of all values
 * it has encountered and returns it as a [Tuple]. Can collect strands of execution in parallel.
 *
 * Only produces a single [Tuple] and converts. Acts as pipeline breaker.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class MergeMeanProjectionOperator(parents: List<Operator>, fields: List<Name.ColumnName>, override val context: QueryContext): Operator.MergingPipelineOperator(parents) {

    /** [MaxProjectionOperator] does act as a pipeline breaker. */
    override val breaker: Boolean = true

    /** Columns produced by [MeanProjectionOperator]. */
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
     * Converts this [MeanProjectionOperator] to a [Flow] and returns it.
     *
     * @return [Flow] representing this [MeanProjectionOperator]
     */
    override fun toFlow(): Flow<Tuple> = channelFlow {
        /* Prepare a global heap selection. */
        val incoming = this@MergeMeanProjectionOperator.parents
        val columns = this@MergeMeanProjectionOperator.columns.toTypedArray()

        /* Prepare holder of type. */
        val globalCount = this@MergeMeanProjectionOperator.parentColumns.map { 0L }.toTypedArray()
        val globalSum: Array<NumericValue<*>> = this@MergeMeanProjectionOperator.parentColumns.map {
            when (it.type) {
                is Types.Byte -> ByteValue.ZERO
                is Types.Short -> ShortValue.ZERO
                is Types.Int -> IntValue.ZERO
                is Types.Long -> LongValue.ZERO
                is Types.Float -> FloatValue.ZERO
                is Types.Double -> DoubleValue.ZERO
                is Types.Complex32 -> Complex32Value.ZERO
                is Types.Complex64 -> Complex64Value.ZERO
                else -> throw ExecutionException.OperatorExecutionException(this@MergeMeanProjectionOperator, "The provided column $it cannot be used for a ${Projection.SUM} projection. ")
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
                val localCount = this@MergeMeanProjectionOperator.parentColumns.map { 0L }.toTypedArray()
                val localSum: Array<NumericValue<*>> = this@MergeMeanProjectionOperator.parentColumns.map {
                    when (it.type) {
                        is Types.Byte -> ByteValue.ZERO
                        is Types.Short -> ShortValue.ZERO
                        is Types.Int -> IntValue.ZERO
                        is Types.Long -> LongValue.ZERO
                        is Types.Float -> FloatValue.ZERO
                        is Types.Double -> DoubleValue.ZERO
                        is Types.Complex32 -> Complex32Value.ZERO
                        is Types.Complex64 -> Complex64Value.ZERO
                        else -> throw ExecutionException.OperatorExecutionException(this@MergeMeanProjectionOperator, "The provided column $it cannot be used for a ${Projection.SUM} projection. ")
                    }
                }.toTypedArray()
                op.toFlow().collect {
                    for ((i,c) in this@MergeMeanProjectionOperator.parentColumns.withIndex()) {
                        localCount[i] += 1L
                        localSum[i] += it[c] as NumericValue<*>
                    }
                }
                mutex.withLock {
                    for (i in this@MergeMeanProjectionOperator.parentColumns.indices) {
                        globalSum[i] += localSum[i]
                        globalCount[i] += localCount[i]
                    }
                }
            }
        }
        jobs.forEach { it.join() } /* Wait for jobs to complete. */

        /** Emit record. */
        val results = Array<Value?>(globalSum.size) { globalSum[it] / LongValue(globalCount[it]) }
        send(StandaloneTuple(0L, columns, results))
    }
}