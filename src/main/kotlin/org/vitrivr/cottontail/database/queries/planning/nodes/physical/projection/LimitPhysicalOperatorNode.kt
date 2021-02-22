package org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.transform.LimitOperator
import kotlin.math.min

/**
 * A [UnaryPhysicalOperatorNode] that represents the application of a LIMIT or SKIP clause on the result.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class LimitPhysicalOperatorNode(val limit: Long, val skip: Long) : UnaryPhysicalOperatorNode() {

    init {
        require(this.limit > 0) { "Limit must be greater than zero but isn't (limit = $limit)." }
        require(this.limit >= 0) { "Skip must be greater or equal to zero but isn't (limit = $skip)." }
    }

    /** The [LimitPhysicalOperatorNode] returns the [ColumnDef] of its input, or no column at all. */
    override val columns: Array<ColumnDef<*>>
        get() = this.input.columns

    override val outputSize: Long
        get() = min((this.input.outputSize - this.skip), this.limit)

    override val cost: Cost
        get() = Cost(cpu = this.outputSize * Cost.COST_MEMORY_ACCESS)

    override fun copy() = LimitPhysicalOperatorNode(this.limit, this.skip)

    override fun toOperator(tx: TransactionContext, ctx: QueryContext) = LimitOperator(this.input.toOperator(tx, ctx), this.skip, this.limit)

    /**
     * Calculates and returns the digest for this [LimitPhysicalOperatorNode].
     *
     * @return Digest for this [LimitPhysicalOperatorNode]e
     */
    override fun digest(): Long {
        var result = super.digest()
        result = 31L * result + this.limit.hashCode()
        result = 31L * result + this.skip.hashCode()
        return result
    }
}