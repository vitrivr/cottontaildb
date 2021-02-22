package org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicateHint
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.projection.DistanceProjectionOperator
import org.vitrivr.cottontail.execution.operators.transform.MergeOperator
import org.vitrivr.cottontail.utilities.math.KnnUtilities

/**
 * A [UnaryPhysicalOperatorNode] that represents the application of a [KnnPredicate] on some intermediate result.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class DistancePhysicalOperatorNode(val predicate: KnnPredicate) : UnaryPhysicalOperatorNode() {

    /** The [DistancePhysicalOperatorNode] returns the [ColumnDef] of its input + a distance column. */
    override val columns: Array<ColumnDef<*>>
        get() = this.input.columns + KnnUtilities.distanceColumnDef(this.predicate.column.name.entity())

    /** The [DistancePhysicalOperatorNode] requires all [ColumnDef]s used in the [KnnPredicate]. */
    override val requires: Array<ColumnDef<*>>
        get() = this.predicate.columns.toTypedArray()

    /** The output size of a [DistancePhysicalOperatorNode] always equal to the . */
    override val outputSize: Long
        get() = this.input.outputSize

    /** The [Cost] of a [DistancePhysicalOperatorNode]. */
    override val cost: Cost
        get() = Cost(cpu = this.input.outputSize * this.predicate.atomicCpuCost)

    override fun copy() = DistancePhysicalOperatorNode(this.predicate)

    override fun bindValues(ctx: QueryContext): OperatorNode {
        this.predicate.bindValues(ctx)
        return super.bindValues(ctx)
    }

    override fun toOperator(tx: TransactionContext, ctx: QueryContext): Operator {
        val hint = this.predicate.hint
        val pMax = tx.availableThreads / 4
        val p = if (hint is KnnPredicateHint.ParallelKnnHint) {
            Integer.max(hint.min, Integer.min(pMax, hint.max))
        } else {
            Integer.min(this.totalCost.parallelisation(), pMax)
        }
        this.predicate.bindValues(ctx)
        return if (p > 1 && this.input.canBePartitioned) {
            val partitions = this.input.partition(p)
            val operators = partitions.map { DistanceProjectionOperator(it.toOperator(tx, ctx), this.predicate) }
            MergeOperator(operators)
        } else {
            DistanceProjectionOperator(this.input.toOperator(tx, ctx), this.predicate)
        }
    }

    /**
     * Calculates and returns the digest for this [KnnPhysicalOperatorNode].
     *
     * @return Digest for this [KnnPhysicalOperatorNode]e
     */
    override fun digest(): Long = 31L * super.digest() + this.predicate.digest()
}