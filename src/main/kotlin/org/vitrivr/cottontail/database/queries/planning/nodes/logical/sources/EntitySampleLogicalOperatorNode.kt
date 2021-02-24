package org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources.EntitySamplePhysicalOperatorNode

/**
 * A [EntitySourceLogicalOperatorNode] that formalizes the sampling of a physical [Entity] in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class EntitySampleLogicalOperatorNode(entity: Entity, columns: Array<ColumnDef<*>>, val size: Long, val seed: Long = System.currentTimeMillis()) : EntitySourceLogicalOperatorNode(entity, columns) {
    /**
     * Returns a copy of this [EntitySampleLogicalOperatorNode] and its input.
     *
     * @return Copy of this [EntitySampleLogicalOperatorNode] and its input.
     */
    override fun copyWithInputs(): EntitySampleLogicalOperatorNode = EntitySampleLogicalOperatorNode(this.entity, this.columns, this.size, this.seed)

    /**
     * Returns a copy of this [EntitySampleLogicalOperatorNode] and its output.
     *
     * @param inputs The [OperatorNode] that should act as inputs.
     * @return Copy of this [EntitySampleLogicalOperatorNode] and its output.
     */
    override fun copyWithOutput(vararg inputs: OperatorNode.Logical): OperatorNode.Logical {
        require(inputs.isEmpty()) { "No input is allowed for nullary operators." }
        val sample = EntitySampleLogicalOperatorNode(this.entity, this.columns, this.size, this.seed)
        return (this.output?.copyWithOutput(sample) ?: sample)
    }

    /**
     * Returns a [EntitySamplePhysicalOperatorNode] representation of this [EntitySampleLogicalOperatorNode]
     *
     * @return [EntitySamplePhysicalOperatorNode]
     */
    override fun implement(): Physical = EntitySamplePhysicalOperatorNode(this.entity, this.columns, this.size, this.seed)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is EntitySampleLogicalOperatorNode) return false
        if (!super.equals(other)) return false

        if (size != other.size) return false
        if (seed != other.seed) return false

        return true
    }

    override fun hashCode(): Int {
        var result = super.hashCode()
        result = 31 * result + size.hashCode()
        result = 31 * result + seed.hashCode()
        return result
    }
}