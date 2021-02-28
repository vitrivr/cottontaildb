package org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.transform.LimitOperator
import kotlin.math.min

/**
 * A [UnaryPhysicalOperatorNode] that represents the application of a LIMIT or SKIP clause on the result.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class LimitPhysicalOperatorNode(input: OperatorNode.Physical, val limit: Long, val skip: Long) : UnaryPhysicalOperatorNode(input) {

    init {
        require(this.limit > 0) { "Limit must be greater than zero but isn't (limit = $limit)." }
        require(this.limit >= 0) { "Skip must be greater or equal to zero but isn't (limit = $skip)." }
    }

    /** The [LimitPhysicalOperatorNode] returns the [ColumnDef] of its input, or no column at all. */
    override val columns: Array<ColumnDef<*>>
        get() = this.input.columns

    /** The output size of this [LimitPhysicalOperatorNode], which depends on skip and limit. */
    override val outputSize: Long = min((this.input.outputSize - this.skip), this.limit)

    /** The [Cost] of a [LimitPhysicalOperatorNode]. */
    override val cost: Cost = Cost(cpu = this.outputSize * Cost.COST_MEMORY_ACCESS)

    /**
     * Returns a copy of this [LimitPhysicalOperatorNode] and its input.
     *
     * @return Copy of this [LimitPhysicalOperatorNode] and its input.
     */
    override fun copyWithInputs() = LimitPhysicalOperatorNode(this.input.copyWithInputs(), this.limit, this.skip)

    /**
     * Returns a copy of this [LimitPhysicalOperatorNode] and its output.
     *
     * @param input The [OperatorNode] that should act as inputs.
     * @return Copy of this [LimitPhysicalOperatorNode] and its output.
     */
    override fun copyWithOutput(input: OperatorNode.Physical?): OperatorNode.Physical {
        require(input != null) { "Input is required for copyWithOutput() on unary physical operator node." }
        val limit = LimitPhysicalOperatorNode(input, this.limit, this.skip)
        return (this.output?.copyWithOutput(limit) ?: limit)
    }

    /**
     * Partitions this [LimitPhysicalOperatorNode].
     *
     * @param p The number of partitions to create.
     * @return List of [OperatorNode.Physical], each representing a partition of the original tree.
     */
    override fun partition(p: Int): List<Physical> = this.input.partition(p).map { LimitPhysicalOperatorNode(it, this.limit, this.skip) }

    /**
     * Converts this [LimitPhysicalOperatorNode] to a [LimitOperator].
     *
     * @param tx The [TransactionContext] used for execution.
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(tx: TransactionContext, ctx: QueryContext) = LimitOperator(this.input.toOperator(tx, ctx), this.skip, this.limit)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is LimitPhysicalOperatorNode) return false

        if (limit != other.limit) return false
        if (skip != other.skip) return false

        return true
    }

    override fun hashCode(): Int {
        var result = limit.hashCode()
        result = 31 * result + skip.hashCode()
        return result
    }
}