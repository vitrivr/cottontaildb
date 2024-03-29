package org.vitrivr.cottontail.dbms.queries.operators.physical.predicates

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.execution.operators.predicates.FilterOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.statistics.selectivity.NaiveSelectivityCalculator
import org.vitrivr.cottontail.dbms.statistics.selectivity.Selectivity

/**
 * A [UnaryPhysicalOperatorNode] that represents application of a [BooleanPredicate] on some intermediate result.
 *
 * @author Ralph Gasser
 * @version 2.9.0
 */
class FilterPhysicalOperatorNode(input: Physical, val predicate: BooleanPredicate) : UnaryPhysicalOperatorNode(input) {
    companion object {
        private const val NODE_NAME = "Filter"
    }

    /** The name of this [FilterPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [FilterPhysicalOperatorNode] requires all [ColumnDef]s used in the [ProximityPredicate]. */
    override val requires: List<Binding.Column> by lazy {
        this.predicate.columns.toList()
    }

    /** The estimated output size of this [FilterPhysicalOperatorNode]. Calculated based on [Selectivity] estimates. */
    context(BindingContext, Tuple)
    override val outputSize: Long
        get() = NaiveSelectivityCalculator.estimate(this.predicate, this.statistics).invoke(this.input.outputSize)

    /** The [Cost] of this [FilterPhysicalOperatorNode]. */
    context(BindingContext, Tuple)
    override val cost: Cost
        get() = this.predicate.cost * this.input.outputSize

    /**
     * Creates and returns a copy of this [FilterPhysicalOperatorNode] using the given parents as input.
     *
     * @param input The [OperatorNode.Physical]s that act as input.
     * @return Copy of this [FilterPhysicalOperatorNode].
     */
    override fun copyWithNewInput(vararg input: Physical): FilterPhysicalOperatorNode {
        require(input.size == 1) { "The input arity for FilterPhysicalOperatorNode.copyWithNewInput() must be 1 but is ${input.size}. This is a programmer's error!"}
        return FilterPhysicalOperatorNode(input = input[0], predicate = this.predicate.copy())
    }


    /**
     * Determines, if this [FilterPhysicalOperatorNode] can be executed in the given [QueryContext].
     *
     * An [FilterPhysicalOperatorNode] is executable if it does not contain any [ComparisonOperator.Match] (which must be pushed-down to a fulltext-index)
     *
     * @param ctx The [QueryContext] to check.
     * @return True i
     */
    override fun canBeExecuted(ctx: QueryContext): Boolean
        = this.predicate.atomics.filterIsInstance<BooleanPredicate.Comparison>().none { it.operator is ComparisonOperator.Match } && super.canBeExecuted(ctx)

    /**
     * Converts this [FilterPhysicalOperatorNode] to a [FilterOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): FilterOperator = FilterOperator(this.input.toOperator(ctx), this.predicate, ctx)

    /** Generates and returns a [String] representation of this [FilterPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.predicate}]"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is FilterPhysicalOperatorNode) return false
        if (this.predicate != other.predicate) return false
        return true
    }

    override fun hashCode(): Int = this.predicate.hashCode()

    /**
     * Generates and returns a [Digest] for this [FilterPhysicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest = this.predicate.hashCode().toLong() + 5L
}