package org.vitrivr.cottontail.dbms.queries.operators.physical.transform

import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.dbms.execution.operators.transform.LimitOperator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.physical.UnaryPhysicalOperatorNode
import kotlin.math.min

/**
 * A [UnaryPhysicalOperatorNode] that represents the application of a LIMIT or SKIP clause on the result.
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
class LimitPhysicalOperatorNode(input: Physical? = null, val limit: Long, val skip: Long) : UnaryPhysicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Fetch"
    }

    /** The name of this [LimitPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The output size of this [LimitPhysicalOperatorNode], which depends on skip and limit. */
    override val outputSize: Long
        get() = min((super.outputSize - this.skip), this.limit)

    /** The [Cost] of a [LimitPhysicalOperatorNode]. */
    override val cost: Cost
        get() = Cost.MEMORY_ACCESS * this.outputSize

    /** The [LimitPhysicalOperatorNode] does not allow for partitioning. */
    override val canBePartitioned: Boolean
        get() = false


    /**
     * Creates and returns a copy of this [LimitPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [LimitPhysicalOperatorNode].
     */
    override fun copy() = LimitPhysicalOperatorNode(limit = this.limit, skip = this.skip)

    /**
     * Converts this [LimitPhysicalOperatorNode] to a [LimitOperator].
     *
     * @param ctx The [TransactionContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext) = LimitOperator(
        this.input?.toOperator(ctx) ?: throw IllegalStateException("Cannot convert disconnected OperatorNode to Operator (node = $this)"),
        this.skip, this.limit
    )

    /** Generates and returns a [String] representation of this [LimitPhysicalOperatorNode]. */
    override fun toString() = "${this.groupId}:Limit[${this.skip},${this.limit}]"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is LimitPhysicalOperatorNode) return false

        if (limit != other.limit) return false
        if (skip != other.skip) return false

        return true
    }

    override fun hashCode(): Int {
        var result = limit.hashCode()
        result = 31 * result + skip.hashCode()
        return result
    }
}