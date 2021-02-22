package org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
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
 * @version 1.2.0
 */
class FetchLogicalOperatorNode(val entity: Entity, val fetch: Array<ColumnDef<*>>) : UnaryLogicalOperatorNode() {

    /** The [FetchLogicalOperatorNode] returns the [ColumnDef] of its input + the columns to be fetched. */
    override val columns: Array<ColumnDef<*>>
        get() = this.input.columns + this.fetch

    override fun copy() = FetchLogicalOperatorNode(this.entity, this.fetch)

    /**
     * Returns a [FetchPhysicalOperatorNode] representation of this [FetchLogicalOperatorNode]
     *
     * @return [FetchPhysicalOperatorNode]
     */
    override fun implement(): Physical = FetchPhysicalOperatorNode(this.entity, this.fetch)

    /**
     * Calculates and returns the digest for this [FetchLogicalOperatorNode].
     *
     * @return Digest for this [FetchLogicalOperatorNode]
     */
    override fun digest(): Long {
        var result = super.digest()
        result = 31L * result + this.entity.hashCode()
        result = 31L * result + this.fetch.contentHashCode()
        return result
    }
}