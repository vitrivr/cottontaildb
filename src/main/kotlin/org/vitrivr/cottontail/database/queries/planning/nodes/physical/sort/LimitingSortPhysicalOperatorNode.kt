package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sort

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.sort.LimitingHeapSortOperator
import org.vitrivr.cottontail.model.exceptions.QueryException
import kotlin.math.min

/**
 * A [UnaryPhysicalOperatorNode] that represents sorting the input by a set of specified [ColumnDef]s but limiting the output to the
 * top K entries. This is semantically equivalent to a ORDER BY XY LIMIT Z. Internally, a heap sort algorithm is employed for sorting.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class LimitingSortPhysicalOperatorNode(input: OperatorNode.Physical, sortOn: Array<Pair<ColumnDef<*>, SortOrder>>, val limit: Long, val skip: Long) : UnaryPhysicalOperatorNode(input) {
    init {
        if (sortOn.isEmpty()) throw QueryException.QuerySyntaxException("At least one column must be specified for sorting.")
    }

    /** The [SortPhysicalOperatorNode] returns the [ColumnDef] of its input. */
    override val columns: Array<ColumnDef<*>>
        get() = this.input.columns

    /** The [LimitingSortPhysicalOperatorNode] requires all [ColumnDef]s used on the ORDER BY clause. */
    override val requires: Array<ColumnDef<*>> = sortOn.map { it.first }.toTypedArray()

    /** The size of the output produced by this [SortPhysicalOperatorNode]. */
    override val outputSize: Long = min((this.input.outputSize - this.skip), this.limit)

    /** The [Cost] incurred by this [SortPhysicalOperatorNode]. */
    override val cost: Cost = Cost(
        cpu = 2 * this.input.outputSize * sortOn.size * Cost.COST_MEMORY_ACCESS,
        memory = (this.columns.map { it.type.physicalSize }.sum() * this.outputSize).toFloat()
    )

    /** A [SortPhysicalOperatorNode] orders the input in by the specified [ColumnDef]s. */
    override val order = sortOn

    /**
     * Returns a copy of this [LimitingSortPhysicalOperatorNode] and its input.
     *
     * @return Copy of this [LimitingSortPhysicalOperatorNode] and its input.
     */
    override fun copyWithInputs(): LimitingSortPhysicalOperatorNode = LimitingSortPhysicalOperatorNode(this.input.copyWithInputs(), this.order, this.outputSize, this.skip)

    /**
     * Returns a copy of this [LimitingSortPhysicalOperatorNode] and its output.
     *
     * @param inputs The [OperatorNode] that should act as inputs.
     * @return Copy of this [LimitingSortPhysicalOperatorNode] and its output.
     */
    override fun copyWithOutput(vararg inputs: OperatorNode.Physical): OperatorNode.Physical {
        require(inputs.size == 1) { "Only one input is allowed for unary operators." }
        val sort = LimitingSortPhysicalOperatorNode(inputs[0], this.order, this.limit, this.skip)
        return (this.output?.copyWithOutput(sort) ?: sort)
    }

    /**
     * Partitions this [LimitingSortPhysicalOperatorNode].
     *
     * @param p The number of partitions to create.
     * @return List of [OperatorNode.Physical], each representing a partition of the original tree.
     */
    override fun partition(p: Int): List<Physical> = input.partition(p).map { LimitingSortPhysicalOperatorNode(it, this.order, this.limit, this.skip) }

    /**
     * Converts this [LimitingSortPhysicalOperatorNode] to a [LimitingHeapSortOperator].
     *
     * @param tx The [TransactionContext] used for execution.
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(tx: TransactionContext, ctx: QueryContext): Operator = LimitingHeapSortOperator(this.input.toOperator(tx, ctx), this.order, this.limit, this.skip)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is LimitingSortPhysicalOperatorNode) return false

        if (skip != other.skip) return false
        if (limit != other.limit) return false
        if (!order.contentEquals(other.order)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = skip.hashCode()
        result = 31 * result + limit.hashCode()
        result = 31 * result + order.contentHashCode()
        return result
    }
}