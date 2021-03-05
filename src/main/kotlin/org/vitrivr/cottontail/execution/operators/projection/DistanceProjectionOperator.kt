package org.vitrivr.cottontail.execution.operators.projection

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.onEach
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.model.values.types.VectorValue
import org.vitrivr.cottontail.utilities.math.KnnUtilities

/**
 * A [Operator.PipelineOperator] used during query execution. It calculates the distance of a specific [ColumnDef]
 * to a set of query vectors and adds a [ColumnDef] that captures that distance. Used for NNS.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class DistanceProjectionOperator(parent: Operator, val knn: KnnPredicate) : Operator.PipelineOperator(parent) {

    /** The columns produced by this [KnnOperator]. */
    override val columns: Array<ColumnDef<*>> = arrayOf(
        *this.parent.columns,
        KnnUtilities.distanceColumnDef(this.knn.column.name.entity())
    )

    /** The [DistanceProjectionOperator] is not a pipeline breaker. */
    override val breaker: Boolean = false

    /**
     * Converts this [KnnOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [KnnOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        /* Obtain parent flow. */
        val parentFlow = this.parent.toFlow(context)

        /* Generate new flow. */
        return if (this.knn.numberOfWeights > 0) {
            flow {
                val values = Array<Value?>(this@DistanceProjectionOperator.columns.size) { null }
                val knn = this@DistanceProjectionOperator.knn
                parentFlow.onEach {
                    var i = 0
                    val value = it[knn.column]
                    val distance = if (value is VectorValue<*>) {
                        knn.distance(knn.query.first(), value, knn.weights.first())
                    } else {
                        DoubleValue.NaN
                    }
                    it.forEach { _, v -> values[i++] = v }
                    values[i] = distance
                    emit(StandaloneRecord(it.tupleId, this@DistanceProjectionOperator.columns, values))
                }.collect()
            }
        } else {
            flow {
                val values = Array<Value?>(this@DistanceProjectionOperator.columns.size) { null }
                val knn = this@DistanceProjectionOperator.knn
                parentFlow.onEach {
                    var i = 0
                    val value = it[knn.column]
                    val distance = if (value is VectorValue<*>) {
                        knn.distance(knn.query.first(), value)
                    } else {
                        DoubleValue.NaN
                    }
                    it.forEach { _, v -> values[i++] = v }
                    values[i] = distance
                    emit(StandaloneRecord(it.tupleId, this@DistanceProjectionOperator.columns, values))
                }.collect()
            }
        }
    }
}