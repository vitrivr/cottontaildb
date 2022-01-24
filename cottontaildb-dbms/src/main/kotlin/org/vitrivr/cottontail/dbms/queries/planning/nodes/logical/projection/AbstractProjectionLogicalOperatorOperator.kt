package org.vitrivr.cottontail.dbms.queries.planning.nodes.logical.projection

import org.vitrivr.cottontail.dbms.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.projection.Projection

/**
 * An abstract [UnaryLogicalOperatorNode] that represents a projection operation involving.
 *
 * @author Ralph Gasser
 * @version 2.2.0
 */
abstract class AbstractProjectionLogicalOperatorOperator(input: Logical? = null, val type: Projection) : UnaryLogicalOperatorNode(input) {
    /** The name of this [DistanceLogicalOperatorNode]. */
    override val name: String
        get() = this.type.label()

    /** Generates and returns a [String] representation of this [SelectProjectionLogicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.columns.joinToString(",") { it.name.toString() }}]"
}