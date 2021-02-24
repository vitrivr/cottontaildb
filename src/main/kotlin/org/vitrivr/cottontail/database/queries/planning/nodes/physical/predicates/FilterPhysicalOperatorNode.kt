package org.vitrivr.cottontail.database.queries.planning.nodes.physical.predicates

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate
import org.vitrivr.cottontail.database.queries.predicates.bool.ComparisonOperator
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.predicates.FilterOperator
import org.vitrivr.cottontail.execution.operators.predicates.ParallelFilterOperator

/**
 * A [UnaryPhysicalOperatorNode] that represents application of a [BooleanPredicate] on some intermediate result.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class FilterPhysicalOperatorNode(input: OperatorNode.Physical, val predicate: BooleanPredicate) : UnaryPhysicalOperatorNode(input) {

    /** The [FilterPhysicalOperatorNode] returns the [ColumnDef] of its input. */
    override val columns: Array<ColumnDef<*>>
        get() = this.input.columns

    /** The [FilterPhysicalOperatorNode] requires all [ColumnDef]s used in the [KnnPredicate]. */
    override val requires: Array<ColumnDef<*>> = this.predicate.columns.toTypedArray()

    /** The [FilterPhysicalOperatorNode] can only be executed if it doesn't contain any [ComparisonOperator.MATCH]. */
    override val executable: Boolean = this.predicate.atomics.none { it.operator == ComparisonOperator.MATCH } && this.input.executable

    /** The output size of this [FilterPhysicalOperatorNode]. TODO: Estimate selectivity of predicate. */
    override val outputSize: Long = this.input.outputSize

    /** The [Cost] of this [FilterPhysicalOperatorNode]. */
    override val cost: Cost = Cost(cpu = this.input.outputSize * this.predicate.atomicCpuCost)

    /**
     * Returns a copy of this [FilterPhysicalOperatorNode] and its input.
     *
     * @return Copy of this [FilterPhysicalOperatorNode] and its input.
     */
    override fun copyWithInputs() = FilterPhysicalOperatorNode(this.input.copyWithInputs(), this.predicate)

    /**
     * Returns a copy of this [FilterPhysicalOperatorNode] and its output.
     *
     * @param inputs The [OperatorNode] that should act as inputs.
     * @return Copy of this [FilterPhysicalOperatorNode] and its output.
     */
    override fun copyWithOutput(vararg inputs: OperatorNode.Physical): OperatorNode.Physical {
        require(inputs.size == 1) { "Only one input is allowed for unary operators." }
        val update = FilterPhysicalOperatorNode(inputs[0], this.predicate)
        return (this.output?.copyWithOutput(update) ?: update)
    }

    /**
     * Partitions this [FilterPhysicalOperatorNode].
     *
     * @param p The number of partitions to create.
     * @return List of [OperatorNode.Physical], each representing a partition of the original tree.
     */
    override fun partition(p: Int): List<Physical> = input.partition(p).map { FilterPhysicalOperatorNode(it, this.predicate) }

    /**
     * Converts this [FilterPhysicalOperatorNode] to a [FilterOperator].
     *
     * @param tx The [TransactionContext] used for execution.
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(tx: TransactionContext, ctx: QueryContext): Operator {
        val parallelisation = this.cost.parallelisation()
        return if (this.canBePartitioned && parallelisation > 1) {
            val operators = this.input.partition(parallelisation).map { it.toOperator(tx, ctx) }
            ParallelFilterOperator(operators, this.predicate.bindValues(ctx))
        } else {
            FilterOperator(this.input.toOperator(tx, ctx), this.predicate.bindValues(ctx))
        }
    }

    /**
     * Binds values from the provided [QueryContext] to this [FilterPhysicalOperatorNode]'s [BooleanPredicate].
     *
     * @param ctx The [QueryContext] used for value binding.
     */
    override fun bindValues(ctx: QueryContext): OperatorNode {
        this.predicate.bindValues(ctx)
        return super.bindValues(ctx) /* Important! */
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is FilterPhysicalOperatorNode) return false
        if (predicate != other.predicate) return false
        return true
    }

    override fun hashCode(): Int = this.predicate.hashCode()
}