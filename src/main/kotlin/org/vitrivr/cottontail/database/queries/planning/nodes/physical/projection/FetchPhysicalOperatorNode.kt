package org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.transform.FetchOperator

/**
 * A [UnaryPhysicalOperatorNode] that represents fetching certain [ColumnDef] from a specific [Entity] and
 * adding them to the list of retrieved [ColumnDef]s.
 *
 * This can be used for late population, which can lead to optimized performance for kNN queries
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class FetchPhysicalOperatorNode(input: OperatorNode.Physical, val entity: Entity, val fetch: Array<ColumnDef<*>>) : UnaryPhysicalOperatorNode(input) {

    /** The [FetchPhysicalOperatorNode] returns the [ColumnDef] of its input + the columns to be fetched. */
    override val columns: Array<ColumnDef<*>>
        get() = this.input.columns + this.fetch

    /** The output size of this [FetchPhysicalOperatorNode], which equals its input's output size. */
    override val outputSize: Long
        get() = this.input.outputSize

    /** The [Cost] of a [FetchPhysicalOperatorNode]. */
    override val cost: Cost = Cost(
        Cost.COST_DISK_ACCESS_READ,
        Cost.COST_MEMORY_ACCESS
    ) * this.input.outputSize * this.fetch.map { this.statistics[it].avgWidth }.sum()

    /**
     * Returns a copy of this [FetchPhysicalOperatorNode] and its input.
     *
     * @return Copy of this [FetchPhysicalOperatorNode] and its input.
     */
    override fun copyWithInputs() = FetchPhysicalOperatorNode(this.input.copyWithInputs(), this.entity, this.fetch)

    /**
     * Returns a copy of this [FetchPhysicalOperatorNode] and its output.
     *
     * @param input The [OperatorNode] that should act as inputs.
     * @return Copy of this [FetchPhysicalOperatorNode] and its output.
     */
    override fun copyWithOutput(input: OperatorNode.Physical?): OperatorNode.Physical {
        require(input != null) { "Input is required for copyWithOutput() on unary physical operator node." }
        val fetch = FetchPhysicalOperatorNode(input, this.entity, this.fetch)
        return (this.output?.copyWithOutput(fetch) ?: fetch)
    }

    /**
     * Partitions this [FetchPhysicalOperatorNode].
     *
     * @param p The number of partitions to create.
     * @return List of [OperatorNode.Physical], each representing a partition of the original tree.
     */
    override fun partition(p: Int): List<Physical> = this.input.partition(p).map { FetchPhysicalOperatorNode(it, this.entity, this.fetch) }

    /**
     * Converts this [FetchPhysicalOperatorNode] to a [FetchOperator].
     *
     * @param tx The [TransactionContext] used for execution.
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(tx: TransactionContext, ctx: QueryContext) = FetchOperator(this.input.toOperator(tx, ctx), this.entity, this.fetch)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is FetchPhysicalOperatorNode) return false

        if (entity != other.entity) return false
        if (!fetch.contentEquals(other.fetch)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = entity.hashCode()
        result = 31 * result + fetch.contentHashCode()
        return result
    }
}