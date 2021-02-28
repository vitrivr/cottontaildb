package org.vitrivr.cottontail.database.queries.planning.nodes.physical.management

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.management.DeleteOperator

/**
 * A [DeletePhysicalOperatorNode] that formalizes a delete operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class DeletePhysicalOperatorNode(input: OperatorNode.Physical, val entity: Entity) : UnaryPhysicalOperatorNode(input) {
    /** The [DeletePhysicalOperatorNode] produces the [ColumnDef]s defined in the [DeleteOperator]. */
    override val columns: Array<ColumnDef<*>> = DeleteOperator.COLUMNS

    /** The [DeletePhysicalOperatorNode] produces a single record. */
    override val outputSize: Long = 1L

    /** The [Cost] of this [DeletePhysicalOperatorNode]. */
    override val cost: Cost = Cost(io = this.entity.numberOfColumns * this.input.outputSize * Cost.COST_DISK_ACCESS_WRITE)

    /** The [DeletePhysicalOperatorNode]s cannot be partitioned. */
    override val canBePartitioned: Boolean = false

    /**
     * Returns a copy of this [DeletePhysicalOperatorNode] and its input.
     *
     * @return Copy of this [DeletePhysicalOperatorNode] and its input.
     */
    override fun copyWithInputs(): DeletePhysicalOperatorNode = DeletePhysicalOperatorNode(this.input.copyWithInputs(), this.entity)

    /**
     * Returns a copy of this [DeletePhysicalOperatorNode] and its output.
     *
     * @param input The [OperatorNode] that should act as inputs.
     * @return Copy of this [DeletePhysicalOperatorNode] and its output.
     */
    override fun copyWithOutput(input: OperatorNode.Physical?): OperatorNode.Physical {
        require(input != null) { "Input is required for copyWithOutput() on unary physical operator node." }
        val delete = DeletePhysicalOperatorNode(input, this.entity)
        return (this.output?.copyWithOutput(delete) ?: delete)
    }

    /**
     * Converts this [DeletePhysicalOperatorNode] to a [DeleteOperator].
     *
     * @param tx The [TransactionContext] used for execution.
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(tx: TransactionContext, ctx: QueryContext): Operator = DeleteOperator(this.input.toOperator(tx, ctx), this.entity)

    /**
     * [DeletePhysicalOperatorNode] cannot be partitioned.
     */
    override fun partition(p: Int): List<Physical> {
        throw UnsupportedOperationException("DeletePhysicalOperatorNode cannot be partitioned.")
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is DeletePhysicalOperatorNode) return false

        if (entity != other.entity) return false

        return true
    }

    override fun hashCode(): Int = this.entity.hashCode()
}