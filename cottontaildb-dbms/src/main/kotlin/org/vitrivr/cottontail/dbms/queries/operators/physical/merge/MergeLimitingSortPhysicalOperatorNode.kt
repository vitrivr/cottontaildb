package org.vitrivr.cottontail.dbms.queries.operators.physical.merge

import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.nodes.traits.*
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.sort.MergeLimitingHeapSortOperator
import org.vitrivr.cottontail.dbms.execution.operators.transform.MergeOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.NAryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sort.LimitingSortPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.statistics.estimateTupleSize
import java.lang.Long.min

/**
 * An [NAryPhysicalOperatorNode] that represents the merging, sorting and limiting of inputs from different strands of execution.
 *
 * This particular operator only exists as physical operator and inherently implies potential for parallelism in an execution plan.
 * Since the collection of the different strands may potentially take place concurrently, the output order of  elements is usually
 * undefined. Furthermore, this operator must orchestrate a switching of [BindingContext] instances.
 *
 * @author Ralph Gasser
 * @version 2.9.0
 */
class MergeLimitingSortPhysicalOperatorNode(vararg inputs: Physical, val sortOn: List<Pair<Binding.Column, SortOrder>>, val limit: Int): NAryPhysicalOperatorNode(*inputs) {

    /** Name of the [MergeLimitingSortPhysicalOperatorNode]. */
    override val name: String = "MergeLimitingSort"

    /** The [MergeLimitingSortPhysicalOperatorNode] depends on all its [GroupId]s. */
    override val dependsOn: Array<GroupId> by lazy {
        inputs.map { it.groupId }.toTypedArray()
    }

    /** The output size of all [MergeLimitingSortPhysicalOperatorNode]s is usually the specified limit. */
    context(BindingContext, Tuple)
    override val outputSize: Long
        get() = min(this.inputs.sumOf { it.outputSize }, this.limit.toLong())

    /** The [Cost] incurred by the [MergeLimitingSortPhysicalOperatorNode] is similar to that of the [LimitingSortPhysicalOperatorNode]. */
    context(BindingContext, Tuple)
    override val cost: Cost
        get() = Cost(
             cpu = 2 * this.inputs.sumOf { it.outputSize } * this.sortOn.size * Cost.MEMORY_ACCESS.cpu,
             memory = this.statistics.estimateTupleSize().toFloat() * this.outputSize
        )

    /** The [Cost] incurred by merging is usually negligible. */
    context(BindingContext, Tuple)
    override val parallelizableCost: Cost
        get() = this.inputs.map { it.totalCost }.reduce {c1, c2 -> c1 + c2}

    /** The [MergeLimitingSortPhysicalOperatorNode] overwrites/sets the [OrderTrait] and the [LimitTrait].  */
    override val traits: Map<TraitType<*>, Trait> by lazy {
       mapOf(
            OrderTrait to OrderTrait(this.sortOn),
            LimitTrait to LimitTrait(this.limit.toLong()),
            MaterializedTrait to MaterializedTrait,
            NotPartitionableTrait to NotPartitionableTrait /* Once explicit sorting has been introduced, no more partitioning is possible. */
        )
    }

    /**
     * Creates and returns a copy of this [MergeLimitingSortPhysicalOperatorNode] using the given parents as input.
     *
     * @param input The [OperatorNode.Physical]s that act as input.
     * @return Copy of this [MergeLimitingSortPhysicalOperatorNode].
     */
    override fun copyWithNewInput(vararg input: Physical) = MergeLimitingSortPhysicalOperatorNode(*input, sortOn = this.sortOn, limit = this.limit)

    /**
     * Converts this [MergeLimitingSortPhysicalOperatorNode] to a [MergeOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator = MergeLimitingHeapSortOperator(this.inputs.map { it.toOperator(ctx.split()) }, this.sortOn, this.limit, ctx)

    /**
     * Generates and returns a [Digest] for this [MergeLimitingSortPhysicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest {
        var result = this.limit.hashCode() + 2L
        result += 31L * result + this.sortOn.hashCode() + 5L
        return result
    }
}