package org.vitrivr.cottontail.database.queries.planning.nodes.logical.sources

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.NullaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources.EntitySamplePhysicalOperatorNode
import org.vitrivr.cottontail.model.basics.Name

/**
 * A [NullaryLogicalOperatorNode] that formalizes the sampling of a physical [Entity] in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 2.2.0
 */
class EntitySampleLogicalOperatorNode(override val groupId: Int, val entity: EntityTx, val fetch: Map<Name.ColumnName,ColumnDef<*>>, val p: Float, val seed: Long = System.currentTimeMillis()) : NullaryLogicalOperatorNode() {

    companion object {
        private const val NODE_NAME = "SampleEntity"
    }

    init {
        require(this.p in 0.0f..1.0f) { "Probability p must be between 0.0 and 1.0 but has value $p."}
    }

    /** The name of this [EntitySampleLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [ColumnDef] produced by this [EntitySampleLogicalOperatorNode]. */
    override val columns: List<ColumnDef<*>> = this.fetch.map { it.value.copy(name = it.key) }

    /**
     * Creates and returns a copy of this [EntitySampleLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [EntitySampleLogicalOperatorNode].
     */
    override fun copy() = EntitySampleLogicalOperatorNode(this.groupId, this.entity, this.fetch, this.p, this.seed)

    /**
     * Returns a [EntitySamplePhysicalOperatorNode] representation of this [EntitySampleLogicalOperatorNode]
     *
     * @return [EntitySamplePhysicalOperatorNode]
     */
    override fun implement(): Physical = EntitySamplePhysicalOperatorNode(this.groupId, this.entity, this.fetch, this.p, this.seed)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is EntitySampleLogicalOperatorNode) return false
        if (!super.equals(other)) return false

        if (this.entity != other.entity) return false
        if (this.columns != other.columns) return false
        if (this.seed != other.seed) return false

        return true
    }

    /** Generates and returns a hash code for this [EntitySampleLogicalOperatorNode]. */
    override fun hashCode(): Int {
        var result = this.entity.hashCode()
        result = 31 * result + this.columns.hashCode()
        result = 31 * result + this.seed.hashCode()
        return result
    }

    /** Generates and returns a [String] representation of this [EntitySampleLogicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.columns.joinToString(",") { it.name.toString() }}]"
}