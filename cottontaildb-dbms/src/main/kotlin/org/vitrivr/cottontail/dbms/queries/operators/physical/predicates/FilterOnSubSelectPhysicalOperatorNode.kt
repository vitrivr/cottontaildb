package org.vitrivr.cottontail.dbms.queries.operators.physical.predicates

import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.nodes.traits.NotPartitionableTrait
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
import org.vitrivr.cottontail.dbms.queries.operators.basics.BinaryPhysicalOperatorNode
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
class FilterOnSubSelectPhysicalOperatorNode(val predicate: BooleanPredicate, left: Physical, right: Physical) : BinaryPhysicalOperatorNode(left, right) {
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
        get() = super.executable && this.predicate.atomics.filterIsInstance<BooleanPredicate.Comparison>().none { it.operator is ComparisonOperator.Binary.Match }

    /** The estimated output size of this [FilterOnSubSelectPhysicalOperatorNode]. Calculated based on [Selectivity] estimates. */
    context(BindingContext,Record)
    override val outputSize: Long
        get() = NaiveSelectivityCalculator
            .estimate(this@FilterOnSubSelectPhysicalOperatorNode.predicate, this@FilterOnSubSelectPhysicalOperatorNode.statistics)
            .invoke(this@FilterOnSubSelectPhysicalOperatorNode.left.outputSize)

    /** The [Cost] of this [FilterOnSubSelectPhysicalOperatorNode]. */
    context(BindingContext,Record)
    override val cost: Cost
        get() = this.predicate.cost * (this.left.outputSize) + this.right.cost

    /** The [Cost] of this [FilterOnSubSelectPhysicalOperatorNode]. */
    context(BindingContext,Record)
    override val parallelizableCost: Cost
        get() = this.left.parallelizableCost * 0.1f + this.right.parallelizableCost

    /** The [FilterOnSubSelectPhysicalOperatorNode] depends on all but the left input. */
    override val dependsOn: Array<GroupId> by lazy {
        arrayOf(this.right.groupId)
    }

    /**
     *
     */
    override val traits: Map<TraitType<*>, Trait>
        get() = super.traits + mapOf(NotPartitionableTrait to NotPartitionableTrait)

    /**
     * Creates and returns a copy of this [FilterOnSubSelectPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [FilterOnSubSelectPhysicalOperatorNode].
     */
    override fun copyWithNewInput(vararg input: Physical): FilterOnSubSelectPhysicalOperatorNode {
        require(input.size == 2) { "The input arity for FilterOnSubSelectPhysicalOperatorNode.copyWithNewInpu() must be 2 but is ${input.size}. This is a programmer's error!"}
        return FilterOnSubSelectPhysicalOperatorNode(left = input[0], right = input[1], predicate = this.predicate)
    }

    /**
     * The [FilterOnSubSelectPhysicalOperatorNode] simply propagates the call up to both branches of the tree.
     *
     * @param ctx [QueryContext] to partition with.
     * @param max The maximum amount of parallelism
     */
    override fun tryPartition(ctx: QueryContext, max: Int): Physical? {
        val left = this.left.tryPartition(ctx, max)
        val right = this.right.tryPartition(ctx, max)
        return if (left != null && right != null) {
            this.copyWithOutput(left, right)
        } else if (left != null) {
            this.copyWithOutput(left, this.right.copyWithExistingInput())
        } else if (right != null) {
            this.copyWithOutput(this.left.copyWithExistingInput(),  right)
        } else {
            null
        }
    }

    /**
     * Converts this [FilterOnSubSelectPhysicalOperatorNode] to a [FilterOnSubselectOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator = FilterOnSubselectOperator(this.left.toOperator(ctx), this.right.toOperator(ctx), this.predicate, ctx)

    /** Generates and returns a [String] representation of this [FilterOnSubSelectPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.predicate}]"

    /**
     * Generates and returns a [Digest] for this [FilterOnSubselectOperator].
     *
     * @return [Digest]
     */
    override fun digest(): Digest = this.predicate.hashCode() + 1L
}