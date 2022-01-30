package org.vitrivr.cottontail.dbms.queries.planning.nodes.physical.predicates

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.dbms.queries.QueryContext
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.dbms.queries.planning.nodes.logical.BinaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.nodes.logical.predicates.FilterOnSubSelectLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.nodes.physical.NAryPhysicalOperatorNode
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.dbms.statistics.selectivity.NaiveSelectivityCalculator
import org.vitrivr.cottontail.dbms.statistics.selectivity.Selectivity
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.predicates.FilterOnSubselectOperator

/**
 * A [BinaryLogicalOperatorNode] that formalizes filtering using some [BooleanPredicate].
 *
 * As opposed to [FilterPhysicalOperatorNode], the [FilterOnSubSelectPhysicalOperatorNode] depends on
 * the execution of one or many sub-queries.
 *
 * @author Ralph Gasser
 * @version 2.2.0
 */
class FilterOnSubSelectPhysicalOperatorNode(val predicate: BooleanPredicate, vararg inputs: Physical) : NAryPhysicalOperatorNode(*inputs) {
    companion object {
        private const val NODE_NAME = "Filter"
    }

    /** The name of this [FilterOnSubSelectPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [FilterOnSubSelectPhysicalOperatorNode] requires all [ColumnDef]s used in the [ProximityPredicate]. */
    override val requires: List<ColumnDef<*>> = this.predicate.columns.toList()

    /** The [FilterOnSubSelectPhysicalOperatorNode] can only be executed if it doesn't contain any [ComparisonOperator.Binary.Match]. */
    override val executable: Boolean
        get() = super.executable && this.predicate.atomics.none { it.operator is ComparisonOperator.Binary.Match }

    /** The estimated output size of this [FilterOnSubSelectPhysicalOperatorNode]. Calculated based on [Selectivity] estimates. */
    override val outputSize: Long
        get() = NaiveSelectivityCalculator.estimate(this.predicate, this.statistics).invoke(this.inputs[0].outputSize)

    /** The [Cost] of this [FilterOnSubSelectPhysicalOperatorNode]. */
    override val cost: Cost
        get() = Cost(cpu = this.predicate.atomicCpuCost) * (this.inputs.firstOrNull()?.outputSize ?: 0)

    /**
     * Creates and returns a copy of this [FilterOnSubSelectPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [FilterOnSubSelectPhysicalOperatorNode].
     */
    override fun copy() = FilterOnSubSelectPhysicalOperatorNode(predicate = this.predicate.copy())

    /**
     * Binds the provided [BindingContext] to this [BooleanPredicate].
     *
     * @param context The new [BindingContext].
     */
    override fun bind(context: BindingContext) {
        super.bind(context)
        this.predicate.bind(context)
    }

    /**
     * Converts this [FilterPhysicalOperatorNode] to a [FilterOnSubselectOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator = FilterOnSubselectOperator(
        this.inputs[0].toOperator(ctx),
        ctx.bindings,
        this.inputs.drop(1).map { it.toOperator(ctx) },
        this.predicate
    )

    /** Generates and returns a [String] representation of this [FilterOnSubSelectLogicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.predicate}]"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is FilterOnSubSelectPhysicalOperatorNode) return false
        if (predicate != other.predicate) return false
        return true
    }

    override fun hashCode(): Int = this.predicate.hashCode()
}