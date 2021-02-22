package org.vitrivr.cottontail.database.queries.planning.nodes.logical

import org.vitrivr.cottontail.database.queries.OperatorNode

/**
 * An abstract [OperatorNode.Logical] implementation that has no input.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class NullaryLogicalOperatorNode : OperatorNode.Logical() {
    /** Input arity of [NullaryLogicalOperatorNode] is always zero. */
    final override val inputArity: Int = 0
}