package org.vitrivr.cottontail.database.queries.planning.nodes.logical.management

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.binding.Binding
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.management.UpdatePhysicalOperatorNode
import org.vitrivr.cottontail.execution.operators.management.UpdateOperator
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A [DeleteLogicalOperatorNode] that formalizes an UPDATE operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class UpdateLogicalOperatorNode(input: OperatorNode.Logical, val entity: Entity, val values: List<Pair<ColumnDef<*>, Binding<Value>>>) : UnaryLogicalOperatorNode(input) {
    /** The [UpdateLogicalOperatorNode] does produce the columns defined in the [UpdateOperator]. */
    override val columns: Array<ColumnDef<*>> = UpdateOperator.COLUMNS

    /**
     * Returns a copy of this [UpdateLogicalOperatorNode]
     *
     * @return Copy of this [UpdateLogicalOperatorNode]
     */
    override fun copyWithInputs(): UpdateLogicalOperatorNode = UpdateLogicalOperatorNode(this.input.copyWithInputs(), this.entity, this.values)

    /**
     * Returns a copy of this [InsertUpdateLogicalOperatorNodeLogicalOperatorNode] and its output.
     *
     * @param input The [OperatorNode.Logical] that should act as inputs.
     * @return Copy of this [UpdateLogicalOperatorNode] and its output.
     */
    override fun copyWithOutput(input: OperatorNode.Logical?): OperatorNode.Logical {
        require(input != null) { "Input is required for unary logical operator node." }
        val update = UpdateLogicalOperatorNode(input, this.entity, this.values)
        return (this.output?.copyWithOutput(update) ?: update)
    }

    /**
     * Returns a [UpdatePhysicalOperatorNode] representation of this [UpdateLogicalOperatorNode]
     *
     * @return [UpdatePhysicalOperatorNode]
     */
    override fun implement() = UpdatePhysicalOperatorNode(this.input.implement(), this.entity, this.values)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is UpdateLogicalOperatorNode) return false

        if (entity != other.entity) return false
        if (values != other.values) return false

        return true
    }

    override fun hashCode(): Int {
        var result = entity.hashCode()
        result = 31 * result + values.hashCode()
        return result
    }
}