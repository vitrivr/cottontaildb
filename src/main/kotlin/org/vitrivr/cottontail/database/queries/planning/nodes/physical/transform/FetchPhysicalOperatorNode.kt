package org.vitrivr.cottontail.database.queries.planning.nodes.physical.transform

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.execution.operators.transform.FetchOperator
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Type

/**
 * A [UnaryPhysicalOperatorNode] that represents fetching certain [ColumnDef] from a specific [Entity] and
 * adding them to the list of retrieved [ColumnDef]s.
 *
 * This can be used for late population, which can lead to optimized performance for kNN queries
 *
 * @author Ralph Gasser
 * @version 2.2.0
 */
class FetchPhysicalOperatorNode(input: Physical? = null, val entity: EntityTx, val fetch: List<Pair<Name.ColumnName,ColumnDef<*>>>) : UnaryPhysicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Fetch"
    }

    /** The name of this [FetchPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [FetchPhysicalOperatorNode] returns the [ColumnDef] of its input + the columns to be fetched. */
    override val columns: List<ColumnDef<*>>
        get() = super.columns + this.fetch.map { it.second.copy(name = it.first) }

    /** The [Cost] of a [FetchPhysicalOperatorNode]. */
    override val cost: Cost
        get() = Cost(Cost.COST_DISK_ACCESS_READ, Cost.COST_MEMORY_ACCESS) * this.outputSize * this.columns.sumOf {
            if (it.type == Type.String) {
                this.statistics[it].avgWidth * Char.SIZE_BYTES
            } else {
                it.type.physicalSize
            }
        }

    /**
     * Creates and returns a copy of this [FetchPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [FetchPhysicalOperatorNode].
     */
    override fun copy() = FetchPhysicalOperatorNode(entity = this.entity, fetch = this.fetch)

    /**
     * Partitions this [FetchPhysicalOperatorNode].
     *
     * @param p The number of partitions to create.
     * @return List of [OperatorNode.Physical], each representing a partition of the original tree.
     */
    override fun partition(p: Int): List<Physical> =
        this.input?.partition(p)?.map { FetchPhysicalOperatorNode(it, this.entity, this.fetch) } ?: throw IllegalStateException("Cannot partition disconnected OperatorNode (node = $this)")

    /**
     * Converts this [FetchPhysicalOperatorNode] to a [FetchOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext) = FetchOperator(
        this.input?.toOperator(ctx) ?: throw IllegalStateException("Cannot convert disconnected OperatorNode to Operator (node = $this)"),
        this.entity,
        this.fetch
    )

    /** Generates and returns a [String] representation of this [FetchPhysicalOperatorNode]. */
    override fun toString() = "${this.groupId}:Fetch[${this.columns.joinToString(",") { it.name.toString() }}]"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is FetchPhysicalOperatorNode) return false

        if (this.entity != other.entity) return false
        if (this.fetch != other.fetch) return false

        return true
    }

    override fun hashCode(): Int {
        var result = this.entity.hashCode()
        result = 31 * result + this.fetch.hashCode()
        return result
    }
}