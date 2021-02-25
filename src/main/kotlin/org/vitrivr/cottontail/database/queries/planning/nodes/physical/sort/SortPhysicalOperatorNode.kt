package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sort

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.sort.HeapSortOperator
import org.vitrivr.cottontail.model.exceptions.QueryException

/**
 * A [UnaryPhysicalOperatorNode] that represents sorting the input by a set of specified [ColumnDef]s. Internally,
 * a heap sort algorithm is applied for sorting.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class SortPhysicalOperatorNode(input: OperatorNode.Physical, sortOn: Array<Pair<ColumnDef<*>, SortOrder>>) : UnaryPhysicalOperatorNode(input) {
    init {
        if (sortOn.isEmpty()) throw QueryException.QuerySyntaxException("At least one column must be specified for sorting.")
    }

    /** The [SortPhysicalOperatorNode] returns the [ColumnDef] of its input. */
    override val columns: Array<ColumnDef<*>>
        get() = this.input.columns

    /** The [SortPhysicalOperatorNode] requires all [ColumnDef]s used on the ORDER BY clause. */
    override val requires: Array<ColumnDef<*>> = sortOn.map { it.first }.toTypedArray()

    /** The size of the output produced by this [SortPhysicalOperatorNode]. */
    override val outputSize: Long
        get() = this.input.outputSize

    /** The [Cost] incurred by this [SortPhysicalOperatorNode]. */
    override val cost: Cost = Cost(
        cpu = 2 * sortOn.size * Cost.COST_MEMORY_ACCESS,
        memory = this.columns.map { this.statistics[it].avgWidth }.sum().toFloat()
    ) * this.outputSize

    /** A [SortPhysicalOperatorNode] orders the input in by the specified [ColumnDef]s. */
    override val order = sortOn

    /**
     * Returns a copy of this [SortPhysicalOperatorNode] and its input.
     *
     * @return Copy of this [SortPhysicalOperatorNode] and its input.
     */
    override fun copyWithInputs(): SortPhysicalOperatorNode = SortPhysicalOperatorNode(this.input.copyWithInputs(), this.order)

    /**
     * Returns a copy of this [SortPhysicalOperatorNode] and its output.
     *
     * @param inputs The [OperatorNode] that should act as inputs.
     * @return Copy of this [SortPhysicalOperatorNode] and its output.
     */
    override fun copyWithOutput(vararg inputs: OperatorNode.Physical): OperatorNode.Physical {
        require(inputs.size == 1) { "Only one input is allowed for unary operators." }
        val sort = SortPhysicalOperatorNode(inputs[0], this.order)
        return (this.output?.copyWithOutput(sort) ?: sort)
    }

    /**
     * Partitions this [SortPhysicalOperatorNode].
     *
     * @param p The number of partitions to create.
     * @return List of [OperatorNode.Physical], each representing a partition of the original tree.
     */
    override fun partition(p: Int): List<Physical> = this.input.partition(p).map { SortPhysicalOperatorNode(it, this.order) }

    /**
     * Converts this [SortPhysicalOperatorNode] to a [HeapSortOperator].
     *
     * @param tx The [TransactionContext] used for execution.
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(tx: TransactionContext, ctx: QueryContext): Operator = HeapSortOperator(
        this.input.toOperator(tx, ctx),
        this.order,
        if (this.outputSize > Integer.MAX_VALUE) {
            Integer.MAX_VALUE
            /** TODO: This case requires special handling. */
        } else {
            this.outputSize.toInt()
        }
    )

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is SortPhysicalOperatorNode) return false

        if (!order.contentEquals(other.order)) return false

        return true
    }

    override fun hashCode(): Int {
        return order.contentHashCode()
    }
}