package org.vitrivr.cottontail.dbms.queries.operators.logical.transform

import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.transform.SkipPhysicalOperatorNode

/**
 * A [UnaryLogicalOperatorNode] that represents the application of a SKIP clause on the final result.
 *
 * @author Ralph Gasser
 * @version 2.4.0
 */
class SkipLogicalOperatorNode(input: Logical, val skip: Long) : UnaryLogicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Skip"
    }

    /** The name of this [SkipLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /**
     * Creates a copy of this [SkipLogicalOperatorNode].
     *
     * @param input The new input [OperatorNode.Logical]
     * @return Copy of this [SkipLogicalOperatorNode]
     */
    override fun copyWithNewInput(vararg input: Logical): SkipLogicalOperatorNode {
        require(input.size == 1) { "The input arity for SkipLogicalOperatorNode.copyWithNewInput() must be 1 but is ${input.size}. This is a programmer's error!"}
        return SkipLogicalOperatorNode(input = input[0], skip = this.skip)
    }

    /**
     * Returns a [SkipLogicalOperatorNode] representation of this [SkipLogicalOperatorNode]
     *
     * @return [SkipLogicalOperatorNode]
     */
    override fun implement(): Physical = SkipPhysicalOperatorNode(this.input.implement(), this.skip)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is LimitLogicalOperatorNode) return false
        if (skip != other.limit) return false
        return true
    }

    /** Generates and returns a hash code for this [SkipLogicalOperatorNode]. */
    override fun hashCode(): Int = this.skip.hashCode() + 2

    /** Generates and returns a [String] representation of this [SkipLogicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.skip}]"
}