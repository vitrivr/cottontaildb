package org.vitrivr.cottontail.dbms.queries.operators.logical.transform

import org.vitrivr.cottontail.dbms.queries.operators.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.transform.LimitPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.transform.SkipPhysicalOperatorNode

/**
 * A [UnaryLogicalOperatorNode] that represents the application of a SKIP clause on the final result.
 *
 * @author Ralph Gasser
 * @version 2.3.0
 */
class SkipLogicalOperatorNode(input: Logical? = null, val skip: Long) : UnaryLogicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Skip"
    }

    /** The name of this [SkipLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /**
     * Creates and returns a copy of this [SkipLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [SkipLogicalOperatorNode].
     */
    override fun copy() = SkipLogicalOperatorNode(skip = this.skip)

    /**
     * Returns a [LimitPhysicalOperatorNode] representation of this [SkipLogicalOperatorNode]
     *
     * @return [LimitPhysicalOperatorNode]
     */
    override fun implement(): Physical = SkipPhysicalOperatorNode(this.input?.implement(), this.skip)

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