package org.vitrivr.cottontail.dbms.queries.planning.nodes.physical.projection

import org.vitrivr.cottontail.dbms.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.projection.Projection

/**
 * An abstract [UnaryPhysicalOperatorNode] that represents a projection operation involving.
 *
 * @author Ralph Gasser
 * @version 2.2.0
 */
abstract class AbstractProjectionPhysicalOperatorNode(input: Physical? = null, val type: Projection) : UnaryPhysicalOperatorNode(input) {

    /** The name of this [DistanceLogicalOperatorNode]. */
    override val name: String
        get() = this.type.label()

    /** Generates and returns a [String] representation of this [AbstractProjectionPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.columns.joinToString(",") { it.name.toString() }}]"
}