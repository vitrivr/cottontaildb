package org.vitrivr.cottontail.dbms.queries.operators.physical.merge

import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.transform.MergeOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.physical.NAryPhysicalOperatorNode

/**
 * An [NAryPhysicalOperatorNode] that represents the merging of inputs from different strands of execution.
 *
 * This particular operator only exists as physical operator and inherently implies potential for parallelism in
 * an execution plan.  Since the collection of the different strands may potentially take place concurrently, the
 * output order of  elements is usually undefined. Furthermore, this operator must orchestrate a switching of
 * [BindingContext] instances.
 *
 * @author Ralph Gasser
 * @version 2.3.0
 */
class MergePhysicalOperator(vararg inputs: Physical): NAryPhysicalOperatorNode(*inputs) {

    /** Name of the [MergePhysicalOperator]. */
    override val name: String = "Merge"

    /** The output size of all [MergePhysicalOperator]s is the sum of all its input's output sizes. */
    override val outputSize: Long
        get() = this.inputs.sumOf { it.outputSize }

    /** The [Cost] incurred by the [MergePhysicalOperator] is usually negligible. */
    override val cost: Cost = Cost.ZERO

    /** The parallelizable portion of the [Cost] incurred by the [MergePhysicalOperator] is the sum of the [Cost]s of all inputs. */
    override val parallelizableCost: Cost
        get() = this.inputs.map { it.totalCost }.reduce {c1, c2 -> c1 + c2}

    /* The [MergePhysicalOperator] eliminates all traits from the incoming nodes. */
    override val traits: Map<TraitType<*>, Trait>
        get() = emptyMap()

    /**
     * Creates and returns a copy of this [MergePhysicalOperator] without any children or parents.
     *
     * @return Copy of this [MergePhysicalOperator].
     */
    override fun copy() = MergePhysicalOperator()


    /**
     * Converts this [MergePhysicalOperator] to a [MergeOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator = MergeOperator(this.inputs.map { it.toOperator(ctx.split()) }, ctx.bindings)
}