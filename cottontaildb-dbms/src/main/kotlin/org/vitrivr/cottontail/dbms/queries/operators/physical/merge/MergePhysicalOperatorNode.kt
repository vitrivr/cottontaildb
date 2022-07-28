package org.vitrivr.cottontail.dbms.queries.operators.physical.merge

import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.transform.MergeOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.NAryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode

/**
 * An [NAryPhysicalOperatorNode] that represents the merging of inputs from different strands of execution.
 *
 * This particular operator only exists as physical operator and inherently implies potential for parallelism in
 * an execution plan.  Since the collection of the different strands may potentially take place concurrently, the
 * output order of  elements is usually undefined. Furthermore, this operator must orchestrate a switching of
 * [BindingContext] instances.
 *
 * @author Ralph Gasser
 * @version 2.4.0
 */
class MergePhysicalOperatorNode(vararg inputs: Physical): NAryPhysicalOperatorNode(*inputs) {

    /** Name of the [MergePhysicalOperatorNode]. */
    override val name: String = "Merge"

    /** The output size of all [MergePhysicalOperatorNode]s is the sum of all its input's output sizes. */
    context(BindingContext,Record)    override val outputSize: Long
        get() = this.inputs.sumOf { it.outputSize }

    /** The [Cost] incurred by the [MergePhysicalOperatorNode] is usually negligible. */
    context(BindingContext,Record)    override val cost: Cost
        get() = Cost.ZERO

    /** The parallelizable portion of the [Cost] incurred by the [MergePhysicalOperatorNode] is the sum of the [Cost]s of all inputs. */
    context(BindingContext,Record)    override val parallelizableCost: Cost
        get() = this.inputs.map { it.totalCost }.reduce {c1, c2 -> c1 + c2}

    /** The [MergePhysicalOperator] eliminates all traits from the incoming nodes. */
    override val traits: Map<TraitType<*>, Trait>
        get() = emptyMap()

    /** The [MergePhysicalOperatorNode] depends on all its [GroupId]s. */
    override val dependsOn: Array<GroupId> by lazy {
        inputs.map { it.groupId }.toTypedArray()
    }

    /**
     * Creates and returns a copy of this [MergePhysicalOperatorNode] using the given parents as input.
     *
     * @param input The [OperatorNode.Physical]s that act as input.
     * @return Copy of this [MergePhysicalOperatorNode].
     */
    override fun copyWithNewInput(vararg input: Physical): NAryPhysicalOperatorNode = MergePhysicalOperatorNode(*input)

    /**
     * Converts this [MergePhysicalOperatorNode] to a [MergeOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator = MergeOperator(this.inputs.map { it.toOperator(ctx.split()) }, ctx)
}