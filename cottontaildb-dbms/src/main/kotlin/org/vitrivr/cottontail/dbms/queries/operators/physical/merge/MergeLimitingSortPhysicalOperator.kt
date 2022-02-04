package org.vitrivr.cottontail.dbms.queries.operators.physical.merge

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.sort.MergeLimitingHeapSortOperator
import org.vitrivr.cottontail.dbms.execution.operators.transform.MergeOperator
import org.vitrivr.cottontail.dbms.queries.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.physical.NAryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.sort.SortOrder

/**
 * An [NAryPhysicalOperatorNode] that represents the merging, sorting and limiting of inputs from different strands of execution.
 *
 * This particular operator only exists as physical operator and inherently implies potential for parallelism in an execution plan.
 * Since the collection of the different strands may potentially take place concurrently, the output order of  elements is usually
 * undefined. Furthermore, this operator must orchestrate a switching of [BindingContext] instances.
 *
 * @author Ralph Gasser
 * @version 2.2.0
 */
class MergeLimitingSortPhysicalOperator(vararg inputs: Physical, override val sortOn: List<Pair<ColumnDef<*>, SortOrder>>, val limit: Long): NAryPhysicalOperatorNode(*inputs) {

    /** Name of the [MergeLimitingSortPhysicalOperator]. */
    override val name: String = "MergeLimitingSort"

    /** The output size of all [MergeLimitingSortPhysicalOperator]s is the sum of all its input's output sizes. */
    override val outputSize: Long
        get() = this.inputs.sumOf { it.outputSize }

    /** The [Cost] incurred by merging is usually negligible. */
    override val cost: Cost = Cost.ZERO

    /** [BindingContext] of this [MergeLimitingSortPhysicalOperator]. */
    private var bindingContext: BindingContext? = null

    /**
     * Creates and returns a copy of this [MergeLimitingSortPhysicalOperator] without any children or parents.
     *
     * @return Copy of this [MergeLimitingSortPhysicalOperator].
     */
    override fun copy() = MergeLimitingSortPhysicalOperator(sortOn = this.sortOn, limit = this.limit)

    /**
     * Propagates the [bind] call up the tree.
     *
     * All input operators receive their own [BindingContext], since [MergeLimitingSortPhysicalOperator] allows for parallelisation.
     *
     * @param context [BindingContext] of this [MergeLimitingSortPhysicalOperator].
     */
    override fun bind(context: BindingContext) {
        this.inputs.forEach { it.bind(context.copy()) }
        this.bindingContext = context
    }


    /**
     * Converts this [MergeLimitingSortPhysicalOperator] to a [MergeOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator = MergeLimitingHeapSortOperator(
        this.inputs.map { it.toOperator(ctx) },
        this.bindingContext ?: throw IllegalStateException("Cannot create MergeOperator without valid binding context."),
        this.sortOn,
        this.limit
    )
}