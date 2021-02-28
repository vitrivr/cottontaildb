package org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection.FetchPhysicalOperatorNode

/**
 * A [UnaryLogicalOperatorNode] that represents fetching certain [ColumnDef] from a specific
 * [Entity] and adding them to the list of requested columns.
 *
 * This can be used for deferred fetching of columns, which can lead to optimized performance for queries
 * that involve pruning the result set (e.g. filters or nearest neighbour search).
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class FetchLogicalOperatorNode(input: OperatorNode.Logical, val entity: Entity, val fetch: Array<ColumnDef<*>>) : UnaryLogicalOperatorNode(input) {

    /** The [FetchLogicalOperatorNode] returns the [ColumnDef] of its input + the columns to be fetched. */
    override val columns: Array<ColumnDef<*>> = this.input.columns + this.fetch

    /**
     * Copies this [FetchLogicalOperatorNode] and its input.
     *
     * @return Copy of this [FetchLogicalOperatorNode] and its input.
     */
    override fun copyWithInputs() = FetchLogicalOperatorNode(this.input.copyWithInputs(), this.entity, this.fetch)

    /**
     * Returns a copy of this [FetchLogicalOperatorNode] and its output.
     *
     * @param input The [OperatorNode.Logical] that should act as inputs.
     * @return Copy of this [FetchLogicalOperatorNode] and its output.
     */
    override fun copyWithOutput(input: OperatorNode.Logical?): OperatorNode.Logical {
        require(input != null) { "Input is required for unary logical operator node." }
        val fetch = FetchLogicalOperatorNode(input, this.entity, this.fetch)
        return (this.output?.copyWithOutput(fetch) ?: fetch)
    }

    /**
     * Returns a [FetchPhysicalOperatorNode] representation of this [FetchLogicalOperatorNode]
     *
     * @return [FetchPhysicalOperatorNode]
     */
    override fun implement(): Physical = FetchPhysicalOperatorNode(this.input.implement(), this.entity, this.fetch)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is FetchLogicalOperatorNode) return false

        if (entity != other.entity) return false
        if (!fetch.contentEquals(other.fetch)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = this.entity.hashCode()
        result = 31 * result + fetch.contentHashCode()
        return result
    }
}