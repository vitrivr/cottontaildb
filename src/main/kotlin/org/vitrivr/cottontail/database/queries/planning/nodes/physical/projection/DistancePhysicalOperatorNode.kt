package org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate
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
 * @version 2.0.0
 */
class DistancePhysicalOperatorNode(input: OperatorNode.Physical, val predicate: KnnPredicate) : UnaryPhysicalOperatorNode(input) {

    /** The [DistancePhysicalOperatorNode] returns the [ColumnDef] of its input + a distance column. */
    override val columns: Array<ColumnDef<*>> = this.input.columns + KnnUtilities.distanceColumnDef(this.predicate.column.name.entity())

    /** The [DistancePhysicalOperatorNode] requires all [ColumnDef]s used in the [KnnPredicate]. */
    override val requires: Array<ColumnDef<*>> = this.predicate.columns.toTypedArray()

    /** The output size of a [DistancePhysicalOperatorNode] always equal to the . */
    override val outputSize: Long
        get() = this.input.outputSize

    /** The [Cost] of a [DistancePhysicalOperatorNode]. */
    override val cost: Cost = Cost(cpu = this.input.outputSize * this.predicate.atomicCpuCost)

    /**
     * Returns a copy of this [DistancePhysicalOperatorNode] and its input.
     *
     * @return Copy of this [DistancePhysicalOperatorNode] and its input.
     */
    override fun copyWithInputs() = DistancePhysicalOperatorNode(this.input.copyWithInputs(), this.predicate)

    /**
     * Returns a copy of this [DistancePhysicalOperatorNode] and its output.
     *
     * @param inputs The [OperatorNode] that should act as inputs.
     * @return Copy of this [DistancePhysicalOperatorNode] and its output.
     */
    override fun copyWithOutput(vararg inputs: OperatorNode.Physical): OperatorNode.Physical {
        require(inputs.size == 1) { "Only one input is allowed for unary operators." }
        val distance = DistancePhysicalOperatorNode(inputs[0], this.predicate)
        return (this.output?.copyWithOutput(distance) ?: distance)
    }

    /**
     * Partitions this [DistancePhysicalOperatorNode].
     *
     * @param p The number of partitions to create.
     * @return List of [OperatorNode.Physical], each representing a partition of the original tree.
     */
    override fun partition(p: Int): List<Physical> = this.input.partition(p).map { DistancePhysicalOperatorNode(it, this.predicate) }

    /**
     * Converts this [DistancePhysicalOperatorNode] to a [DistanceProjectionOperator].
     *
     * @param tx The [TransactionContext] used for execution.
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(tx: TransactionContext, ctx: QueryContext): Operator {
        val hint = this.predicate.hint
        val p = if (hint is KnnPredicateHint.ParallelKnnHint) {
            Integer.max(hint.min, this.totalCost.parallelisation(hint.max))
        } else {
            this.totalCost.parallelisation()
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
     * Binds values from the provided [QueryContext] to this [DistancePhysicalOperatorNode]'s [BooleanPredicate].
     *
     * @param ctx The [QueryContext] used for value binding.
     */
    override fun bindValues(ctx: QueryContext): OperatorNode {
        this.predicate.bindValues(ctx)
        return super.bindValues(ctx) /* Important! */
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is DistancePhysicalOperatorNode) return false

        if (predicate != other.predicate) return false

        return true
    }

    override fun hashCode(): Int {
        return predicate.hashCode()
    }
}