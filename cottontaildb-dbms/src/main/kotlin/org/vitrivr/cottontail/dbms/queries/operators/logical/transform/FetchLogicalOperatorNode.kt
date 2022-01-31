package org.vitrivr.cottontail.dbms.queries.operators.logical.transform

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.queries.operators.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.transform.FetchPhysicalOperatorNode

/**
 * A [UnaryLogicalOperatorNode] that represents fetching certain [ColumnDef] from a specific
 * [Entity] and adding them to the list of requested columns.
 *
 * This can be used for deferred fetching of columns, which can lead to optimized performance for queries
 * that involve pruning the result set (e.g. filters or nearest neighbour search).
 *
 * @author Ralph Gasser
 * @version 2.4.0
 */
class FetchLogicalOperatorNode(input: Logical? = null, val entity: EntityTx, val fetch: List<Pair<Binding.Column, ColumnDef<*>>>) : UnaryLogicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Fetch"
    }

    /** The name of this [FetchLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [FetchLogicalOperatorNode] accesses the [ColumnDef] of its input + the columns to be fetched. */
    override val physicalColumns: List<ColumnDef<*>>
        get() = super.physicalColumns + this.fetch.map { it.second }

    /** The [FetchLogicalOperatorNode] returns the [ColumnDef] of its input + the columns to be fetched. */
    override val columns: List<ColumnDef<*>>
        get() = super.columns + this.fetch.map { it.first.column }

    /**
     * Creates and returns a copy of this [LimitLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [LimitLogicalOperatorNode].
     */
    override fun copy() = FetchLogicalOperatorNode(entity = this.entity, fetch = this.fetch)

    /**
     * Returns a [FetchPhysicalOperatorNode] representation of this [FetchLogicalOperatorNode]
     *
     * @return [FetchPhysicalOperatorNode]
     */
    override fun implement(): Physical = FetchPhysicalOperatorNode(this.input?.implement(), this.entity, this.fetch)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is FetchLogicalOperatorNode) return false

        if (this.entity != other.entity) return false
        if (this.fetch != other.fetch) return false

        return true
    }

    /** Generates and returns a [String] representation of this [FetchLogicalOperatorNode]. */
    override fun hashCode(): Int {
        var result = this.entity.hashCode()
        result = 31 * result + this.fetch.hashCode()
        return result
    }

    /** Generates and returns a [String] representation of this [FetchLogicalOperatorNode]. */
    override fun toString() = "${super.toString()}(${this.fetch.joinToString(",") { it.second.name.toString() }})"
}