package org.vitrivr.cottontail.dbms.queries.planning.nodes.logical.transform

import org.vitrivr.cottontail.dbms.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.nodes.physical.transform.LimitPhysicalOperatorNode

/**
 * A [UnaryLogicalOperatorNode] that represents the application of a LIMIT and/or SKIP clause on the final result.
 *
 * @author Ralph Gasser
 * @version 2.2.0
 */
class LimitLogicalOperatorNode(input: Logical? = null, val limit: Long, val skip: Long) : UnaryLogicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Limit"
    }

    /** The name of this [LimitLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /**
     * Creates and returns a copy of this [LimitLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [LimitLogicalOperatorNode].
     */
    override fun copy() = LimitLogicalOperatorNode(limit = this.limit, skip = this.skip)

    /**
     * Returns a [LimitPhysicalOperatorNode] representation of this [LimitLogicalOperatorNode]
     *
     * @return [LimitPhysicalOperatorNode]
     */
    override fun implement(): Physical = LimitPhysicalOperatorNode(this.input?.implement(), this.limit, this.skip)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is LimitLogicalOperatorNode) return false

        if (limit != other.limit) return false
        if (skip != other.skip) return false

        return true
    }

    /** Generates and returns a hash code for this [LimitLogicalOperatorNode]. */
    override fun hashCode(): Int {
        var result = limit.hashCode()
        result = 27 * result + skip.hashCode()
        return result
    }

    /** Generates and returns a [String] representation of this [LimitLogicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.skip},${this.limit}]"
}