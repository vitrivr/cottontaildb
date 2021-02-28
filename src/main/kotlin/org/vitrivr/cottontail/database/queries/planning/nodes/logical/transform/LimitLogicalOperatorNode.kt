package org.vitrivr.cottontail.database.queries.planning.nodes.logical.transform

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection.LimitPhysicalOperatorNode

/**
 * A [UnaryLogicalOperatorNode] that represents the application of a LIMIT and/or SKIP clause on the final result.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class LimitLogicalOperatorNode(input: OperatorNode.Logical, val limit: Long, val skip: Long) : UnaryLogicalOperatorNode(input) {

    init {
        require(this.limit > 0) { "Limit must be greater than zero but isn't (limit = $limit)." }
        require(this.skip >= 0) { "Skip must be greater or equal to zero but isn't (limit = $skip)." }
    }

    /** The [LimitLogicalOperatorNode] returns the [ColumnDef] of its input, or no column at all. */
    override val columns: Array<ColumnDef<*>>
        get() = this.input.columns

    /**
     * Returns a copy of this [LimitLogicalOperatorNode] and its input.
     *
     * @return Copy of this [LimitLogicalOperatorNode] and its input.
     */
    override fun copyWithInputs(): LimitLogicalOperatorNode = LimitLogicalOperatorNode(this.input.copyWithInputs(), this.limit, this.skip)

    /**
     * Returns a copy of this [LimitLogicalOperatorNode] and its output.
     *
     * @param input The [OperatorNode.Logical] that should act as inputs.
     * @return Copy of this [LimitLogicalOperatorNode] and its output.
     */
    override fun copyWithOutput(input: OperatorNode.Logical?): OperatorNode.Logical {
        require(input != null) { "Input is required for unary logical operator node." }
        val limit = LimitLogicalOperatorNode(input, this.limit, this.skip)
        return (this.output?.copyWithOutput(limit) ?: limit)
    }

    /**
     * Returns a [LimitPhysicalOperatorNode] representation of this [LimitLogicalOperatorNode]
     *
     * @return [LimitPhysicalOperatorNode]
     */
    override fun implement(): Physical = LimitPhysicalOperatorNode(this.input.implement(), this.limit, this.skip)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is LimitLogicalOperatorNode) return false

        if (limit != other.limit) return false
        if (skip != other.skip) return false

        return true
    }

    override fun hashCode(): Int {
        var result = limit.hashCode()
        result = 27 * result + skip.hashCode()
        return result
    }
}