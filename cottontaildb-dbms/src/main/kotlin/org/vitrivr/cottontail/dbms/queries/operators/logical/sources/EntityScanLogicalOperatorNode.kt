package org.vitrivr.cottontail.dbms.queries.operators.logical.sources

import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.queries.operators.basics.NullaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sources.EntityScanPhysicalOperatorNode

/**
 * A [NullaryLogicalOperatorNode] that formalizes the scan of a physical [Entity] in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 2.9.0
 */
class EntityScanLogicalOperatorNode(override val groupId: Int, val entity: EntityTx, override val columns: List<Binding.Column>) : NullaryLogicalOperatorNode() {

    companion object {
        private const val NODE_NAME = "ScanEntity"
    }

    init {
        require(this.columns.all { it.physical != null }) { "EntityScanLogicalOperatorNode can only fetch physical columns."  }
    }

    /** The name of this [EntityScanLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /**
     * Creates and returns a copy of this [EntityScanLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [EntityScanLogicalOperatorNode].
     */
    override fun copy() = EntityScanLogicalOperatorNode(this.groupId, this.entity, this.columns.map { it.copy() })

    /**
     * Returns a [EntityScanPhysicalOperatorNode] representation of this [EntityScanLogicalOperatorNode]
     *
     * @return [EntityScanPhysicalOperatorNode]
     */
    override fun implement(): Physical = EntityScanPhysicalOperatorNode(this.groupId, this.entity, this.columns)

    /** Generates and returns a [String] representation of this [EntitySampleLogicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.columns.joinToString(",") { it.physical?.name.toString() }}]"

    /**
     * Generates and returns a [Digest] for this [EntityScanLogicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest {
        var result = this.entity.dbo.name.hashCode() + 1L
        result += 33L * result + this.columns.hashCode()
        return result
    }
}