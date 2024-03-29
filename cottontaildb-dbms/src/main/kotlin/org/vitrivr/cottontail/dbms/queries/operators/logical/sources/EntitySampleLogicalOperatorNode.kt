package org.vitrivr.cottontail.dbms.queries.operators.logical.sources

import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.queries.operators.basics.NullaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sources.EntitySamplePhysicalOperatorNode

/**
 * A [NullaryLogicalOperatorNode] that formalizes the sampling of a physical [Entity] in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 2.9.0
 */
class EntitySampleLogicalOperatorNode(override val groupId: Int, val entity: EntityTx, override val columns: List<Binding.Column>, val p: Float, val seed: Long = System.currentTimeMillis()) : NullaryLogicalOperatorNode() {

    companion object {
        private const val NODE_NAME = "SampleEntity"
    }

    init {
        require(this.p in 0.0f..1.0f) { "Probability p must be between 0.0 and 1.0 but has value $p."}
        require(this.columns.all { it.physical != null }) { "EntitySampleLogicalOperatorNode can only fetch physical columns."  }
    }

    /** The name of this [EntitySampleLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /**
     * Creates and returns a copy of this [EntitySampleLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [EntitySampleLogicalOperatorNode].
     */
    override fun copy() = EntitySampleLogicalOperatorNode(this.groupId, this.entity, this.columns, this.p, this.seed)

    /**
     * Returns a [EntitySamplePhysicalOperatorNode] representation of this [EntitySampleLogicalOperatorNode]
     *
     * @return [EntitySamplePhysicalOperatorNode]
     */
    override fun implement(): Physical = EntitySamplePhysicalOperatorNode(this.groupId, this.entity, this.columns, this.p, this.seed)

    /** Generates and returns a [String] representation of this [EntitySampleLogicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.columns.joinToString(",") { it.physical?.name.toString() }}]"

    /**
     * Generates and returns a [Digest] for this [EntitySampleLogicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest {
        var result = this.entity.dbo.name.hashCode() + 2L
        result += 33L * result + this.p.hashCode()
        result += 33L * result + this.columns.hashCode()
        return result
    }
}