package org.vitrivr.cottontail.database.queries.planning.nodes.physical

import org.vitrivr.cottontail.database.queries.OperatorNode

/**
 * An abstract [PhysicalOperatorNode] implementation that has no input node.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
abstract class NullaryPhysicalOperatorNode : OperatorNode.Physical() {
    /** The arity of the [NullaryPhysicalOperatorNode] is always on. */
    override val inputArity = 0
}