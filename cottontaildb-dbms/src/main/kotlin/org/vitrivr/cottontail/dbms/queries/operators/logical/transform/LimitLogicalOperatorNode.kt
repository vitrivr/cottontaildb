package org.vitrivr.cottontail.dbms.queries.operators.logical.transform

import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.transform.LimitPhysicalOperatorNode

/**
 * A [UnaryLogicalOperatorNode] that represents the application of a LIMIT clause on the final result.
 *
 * @author Ralph Gasser
 * @version 2.4.0
 */
class LimitLogicalOperatorNode(input: Logical, val limit: Long) : UnaryLogicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Limit"
    }

    /** The name of this [LimitLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /**
     * Creates a copy of this [LimitLogicalOperatorNode].
     *
     * @param input The new input [OperatorNode.Logical]
     * @return Copy of this [LimitLogicalOperatorNode]
     */
    override fun copyWithNewInput(vararg input: Logical): LimitLogicalOperatorNode {
        require(input.size == 1) { "The input arity for LimitLogicalOperatorNode.copyWithNewInput() must be 1 but is ${input.size}. This is a programmer's error!"}
        return LimitLogicalOperatorNode(input = input[0], limit = this.limit)
    }

    /**
     * Returns a [LimitPhysicalOperatorNode] representation of this [LimitLogicalOperatorNode]
     *
     * @return [LimitPhysicalOperatorNode]
     */
    override fun implement(): Physical = LimitPhysicalOperatorNode(this.input.implement(), this.limit)

    /** Generates and returns a [String] representation of this [LimitLogicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.limit}]"

    /**
     * Generates and returns a hash code for this [LimitLogicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest = this.limit.hashCode() + 5L
}