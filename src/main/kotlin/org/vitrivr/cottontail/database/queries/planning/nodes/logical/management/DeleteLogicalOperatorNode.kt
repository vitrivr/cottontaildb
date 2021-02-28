package org.vitrivr.cottontail.database.queries.planning.nodes.logical.management

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.management.DeletePhysicalOperatorNode
import org.vitrivr.cottontail.execution.operators.management.DeleteOperator

/**
 * A [DeleteLogicalOperatorNode] that formalizes a DELETE operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class DeleteLogicalOperatorNode(input: OperatorNode.Logical, val entity: Entity) : UnaryLogicalOperatorNode(input) {
    /** The [DeleteLogicalOperatorNode] produces the columns defined in the [DeleteOperator] */
    override val columns: Array<ColumnDef<*>> = DeleteOperator.COLUMNS

    /**
     * Returns a copy of this [DeleteLogicalOperatorNode] and its input.
     *
     * @return Copy of this [DeleteLogicalOperatorNode] and its input.
     */
    override fun copyWithInputs(): DeleteLogicalOperatorNode = DeleteLogicalOperatorNode(this.input.copyWithInputs(), this.entity)

    /**
     * Returns a copy of this [DeleteLogicalOperatorNode] and its output.
     *
     * @param input The [OperatorNode] that should act as inputs.
     * @return Copy of this [DeleteLogicalOperatorNode] and its output.
     */
    override fun copyWithOutput(input: OperatorNode.Logical?): OperatorNode.Logical {
        require(input != null) { "Input is required for unary logical operator node." }
        val delete = DeleteLogicalOperatorNode(input, this.entity)
        return (this.output?.copyWithOutput(delete) ?: delete)
    }

    /**
     * Returns a [DeletePhysicalOperatorNode] representation of this [DeleteLogicalOperatorNode]
     *
     * @return [DeletePhysicalOperatorNode]
     */
    override fun implement() = DeletePhysicalOperatorNode(this.input.implement(), this.entity)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is DeleteLogicalOperatorNode) return false

        if (entity != other.entity) return false
        if (!columns.contentEquals(other.columns)) return false

        return true
    }

    override fun hashCode(): Int {
        var result = entity.hashCode()
        result = 31 * result + columns.contentHashCode()
        return result
    }
}