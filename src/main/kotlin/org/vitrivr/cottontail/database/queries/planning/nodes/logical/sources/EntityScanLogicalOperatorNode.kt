package org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources.EntityScanPhysicalOperatorNode

/**
 * A [EntitySourceLogicalOperatorNode] that formalizes the scan of a physical [Entity] in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class EntityScanLogicalOperatorNode(groupId: Int, entity: Entity, columns: Array<ColumnDef<*>>) : EntitySourceLogicalOperatorNode(groupId, entity, columns) {
    /**
     * Returns a copy of this [EntityScanLogicalOperatorNode] and its input.
     *
     * @return Copy of this [EntityScanLogicalOperatorNode] and its input.
     */
    override fun copyWithInputs(): EntityScanLogicalOperatorNode = EntityScanLogicalOperatorNode(this.groupId, this.entity, this.columns)

    /**
     * Returns a copy of this [EntityScanLogicalOperatorNode] and its output.
     *
     * @param input The [OperatorNode] that should act as inputs.
     * @return Copy of this [EntityScanLogicalOperatorNode] and its output.
     */
    override fun copyWithOutput(input: OperatorNode.Logical?): OperatorNode.Logical {
        require(input == null) { "No input is allowed for copyWithOutput() on nullary logical operator node." }
        val scan = EntityScanLogicalOperatorNode(this.groupId, this.entity, this.columns)
        return (this.output?.copyWithOutput(scan) ?: scan)
    }

    /**
     * Returns a [EntityScanPhysicalOperatorNode] representation of this [EntityScanLogicalOperatorNode]
     *
     * @return [EntityScanPhysicalOperatorNode]
     */
    override fun implement(): Physical = EntityScanPhysicalOperatorNode(this.groupId, this.entity, this.columns)
}