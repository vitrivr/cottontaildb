package org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources.EntityScanPhysicalOperatorNode

/**
 * A [EntitySourceLogicalOperatorNode] that formalizes the scan of a physical [Entity] in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class EntityScanLogicalOperatorNode(entity: Entity, columns: Array<ColumnDef<*>>) : EntitySourceLogicalOperatorNode(entity, columns) {
    /**
     * Returns a copy of this [EntityScanLogicalOperatorNode]
     *
     * @return Copy of this [EntityScanLogicalOperatorNode]
     */
    override fun copy(): EntityScanLogicalOperatorNode = EntityScanLogicalOperatorNode(this.entity, this.columns)

    /**
     * Returns a [EntityScanPhysicalOperatorNode] representation of this [EntityScanLogicalOperatorNode]
     *
     * @return [EntityScanPhysicalOperatorNode]
     */
    override fun implement(): Physical = EntityScanPhysicalOperatorNode(this.entity, this.columns)

    /**
     * Calculates and returns the digest for this [EntitySourceLogicalOperatorNode].
     *
     * @return Digest for this [EntitySourceLogicalOperatorNode]
     */
    override fun digest(): Long {
        var result = super.digest()
        result = 31L * result + this.entity.hashCode()
        result = 31L * result + this.columns.contentHashCode()
        return result
    }
}