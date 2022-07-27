package org.vitrivr.cottontail.dbms.queries.operators.physical.predicates

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.predicates.FilterOnSubselectOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.BinaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.NAryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.statistics.selectivity.NaiveSelectivityCalculator
import org.vitrivr.cottontail.dbms.statistics.selectivity.Selectivity

/**
 * A [BinaryLogicalOperatorNode] that formalizes filtering using some [BooleanPredicate].
 *
 * As opposed to [FilterPhysicalOperatorNode], the [FilterOnSubSelectPhysicalOperatorNode] depends on
 * the execution of one or many sub-queries.
 *
 * @author Ralph Gasser
 * @version 2.4.0
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
        get() = this.predicate.cost * (this.inputs.firstOrNull()?.outputSize ?: 0)

    /** The [FilterOnSubSelectPhysicalOperatorNode] inherits its traits from its left most input. */
    override val traits: Map<TraitType<*>, Trait>
        get() = super.inputs[0].traits

    /** The [FilterOnSubSelectPhysicalOperatorNode] depends on all but the first [inputs]. */
    override val dependsOn: Array<GroupId> by lazy {
        this.inputs.drop(1).map { it.groupId }.toTypedArray()
    }

    /**
     * Creates and returns a copy of this [FilterOnSubSelectPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [FilterOnSubSelectPhysicalOperatorNode].
     */
    override fun copyWithNewInput(vararg input: Physical) = FilterOnSubSelectPhysicalOperatorNode(inputs = input, predicate = this.predicate.copy())

    /**
     * Converts this [FilterOnSubSelectPhysicalOperatorNode] to a [FilterOnSubselectOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator {
        /* Bind predicate to context. */
        this.predicate.bind(ctx.bindings)

        /* Generate and return FilterOnSubselectOperator. */
        return FilterOnSubselectOperator(this.inputs[0].toOperator(ctx), this.inputs.drop(1).map { it.toOperator(ctx) }, this.predicate)
    }

    /** Generates and returns a [String] representation of this [FilterOnSubSelectPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.predicate}]"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is FilterOnSubSelectPhysicalOperatorNode) return false
        if (predicate != other.predicate) return false
        return true
    }

    override fun hashCode(): Int = this.predicate.hashCode()
}