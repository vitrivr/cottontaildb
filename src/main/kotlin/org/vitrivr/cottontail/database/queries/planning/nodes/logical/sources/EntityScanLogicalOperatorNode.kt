package org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.NullaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources.EntityScanPhysicalOperatorNode
import org.vitrivr.cottontail.model.basics.Name

/**
 * A [NullaryLogicalOperatorNode] that formalizes the scan of a physical [Entity] in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 2.2.0
 */
class EntityScanLogicalOperatorNode(override val groupId: Int, val entity: EntityTx, val fetch: List<Pair<Name.ColumnName,ColumnDef<*>>>) : NullaryLogicalOperatorNode() {

    companion object {
        private const val NODE_NAME = "ScanEntity"
    }

    /** The name of this [EntityScanLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [ColumnDef] produced by this [EntitySampleLogicalOperatorNode]. */
    override val columns: List<ColumnDef<*>> = this.fetch.map { it.second.copy(name = it.first) }

    /**
     * Creates and returns a copy of this [EntityScanLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [EntityScanLogicalOperatorNode].
     */
    override fun copy() = EntityScanLogicalOperatorNode(this.groupId, this.entity, this.fetch)

    /**
     * Returns a [EntityScanPhysicalOperatorNode] representation of this [EntityScanLogicalOperatorNode]
     *
     * @return [EntityScanPhysicalOperatorNode]
     */
    override fun implement(): Physical = EntityScanPhysicalOperatorNode(this.groupId, this.entity, this.fetch)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is EntityScanLogicalOperatorNode) return false

        if (this.entity != other.entity) return false
        if (this.columns != other.columns) return false

        return true
    }

    override fun hashCode(): Int {
        var result = entity.hashCode()
        result = 31 * result + this.columns.hashCode()
        return result
    }

    /** Generates and returns a [String] representation of this [EntitySampleLogicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.columns.joinToString(",") { it.name.toString() }}]"
}