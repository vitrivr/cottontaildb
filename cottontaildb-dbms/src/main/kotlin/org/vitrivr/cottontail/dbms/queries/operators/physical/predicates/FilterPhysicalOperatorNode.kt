package org.vitrivr.cottontail.dbms.queries.operators.physical.predicates

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.dbms.execution.operators.predicates.FilterOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.statistics.selectivity.NaiveSelectivityCalculator
import org.vitrivr.cottontail.dbms.statistics.selectivity.Selectivity

/**
 * A [UnaryPhysicalOperatorNode] that represents application of a [BooleanPredicate] on some intermediate result.
 *
 * @author Ralph Gasser
 * @version 2.3.0
 */
class FilterPhysicalOperatorNode(input: Physical? = null, val predicate: BooleanPredicate) : UnaryPhysicalOperatorNode(input) {
    companion object {
        private const val NODE_NAME = "Filter"
    }

    /** The name of this [FilterOnSubSelectPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [FilterPhysicalOperatorNode] requires all [ColumnDef]s used in the [ProximityPredicate]. */
    override val requires: List<ColumnDef<*>> = this.predicate.columns.toList()

    /** The [FilterPhysicalOperatorNode] can only be executed if it doesn't contain any [ComparisonOperator.Binary.Match]. */
    override val executable: Boolean
        get() = super.executable && this.predicate.atomics.none { it.operator is ComparisonOperator.Binary.Match }

    /** The estimated output size of this [FilterOnSubSelectPhysicalOperatorNode]. Calculated based on [Selectivity] estimates. */
    override val outputSize: Long by lazy {
        NaiveSelectivityCalculator.estimate(this.predicate, this.statistics).invoke(this.input?.outputSize ?: 0)
    }

    /** The [Cost] of this [FilterPhysicalOperatorNode]. */
    override val cost: Cost by lazy {
        this.predicate.cost * (this.input?.outputSize ?: 0)
    }

    /**
     * Creates and returns a copy of this [FilterPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [FilterPhysicalOperatorNode].
     */
    override fun copy() = FilterPhysicalOperatorNode(predicate = this.predicate.copy())

    /**
     * Converts this [FilterPhysicalOperatorNode] to a [FilterOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): FilterOperator {
        /* Bind predicate to context. */
        this.predicate.bind(ctx.bindings)

        /* Generate and return FilterOperator. */
        val input = this.input?.toOperator(ctx) ?: throw IllegalStateException("Cannot convert disconnected OperatorNode to Operator (node = $this)")
        return FilterOperator(input, this.predicate)
    }

    /** Generates and returns a [String] representation of this [FilterPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.predicate}]"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is FilterPhysicalOperatorNode) return false
        if (this.predicate != other.predicate) return false
        return true
    }

    override fun hashCode(): Int = this.predicate.hashCode()
}