package org.vitrivr.cottontail.database.queries.planning.nodes.logical.transform
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection.LimitPhysicalOperatorNode

/**
 * A [UnaryLogicalOperatorNode] that represents the application of a LIMIT and/or SKIP clause on the
 * final result [org.vitrivr.cottontail.model.recordset.Recordset].
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class LimitLogicalOperatorNode(val limit: Long, val skip: Long) : UnaryLogicalOperatorNode() {

    init {
        require(this.limit > 0) { "Limit must be greater than zero but isn't (limit = $limit)." }
        require(this.limit >= 0) { "Skip must be greater or equal to zero but isn't (limit = $skip)." }
    }

    /** The [LimitLogicalOperatorNode] returns the [ColumnDef] of its input, or no column at all. */
    override val columns: Array<ColumnDef<*>>
        get() = this.input.columns

    /**
     * Returns a copy of this [LimitLogicalOperatorNode]
     *
     * @return Copy of this [LimitLogicalOperatorNode]
     */
    override fun copy(): LimitLogicalOperatorNode = LimitLogicalOperatorNode(this.limit, this.skip)

    /**
     * Returns a [LimitPhysicalOperatorNode] representation of this [LimitLogicalOperatorNode]
     *
     * @return [LimitPhysicalOperatorNode]
     */
    override fun implement(): Physical = LimitPhysicalOperatorNode(this.limit, this.skip)

    /**
     * Calculates and returns the digest for this [LimitLogicalOperatorNode].
     *
     * @return Digest for this [LimitLogicalOperatorNode]
     */
    override fun digest(): Long {
        var result = super.digest()
        result = 31L * result + this.limit.hashCode()
        result = 31L * result + this.skip.hashCode()
        return result
    }
}