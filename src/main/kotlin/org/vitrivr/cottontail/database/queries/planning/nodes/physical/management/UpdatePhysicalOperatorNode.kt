package org.vitrivr.cottontail.database.queries.planning.nodes.physical.management

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.Binding
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.execution.TransactionContext

import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.management.UpdateOperator
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A [UpdatePhysicalOperatorNode] that formalizes a UPDATE operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class UpdatePhysicalOperatorNode(input: OperatorNode.Physical, val entity: Entity, val values: List<Pair<ColumnDef<*>, Binding<Value>>>) : UnaryPhysicalOperatorNode(input) {

    /** The [UpdatePhysicalOperatorNode] produces the [ColumnDef]s defined in the [UpdateOperator]. */
    override val columns: Array<ColumnDef<*>> = UpdateOperator.COLUMNS

    /** The [UpdatePhysicalOperatorNode] produces a single record. */
    override val outputSize: Long = 1L

    /** The [Cost] of this [UpdatePhysicalOperatorNode]. */
    override val cost: Cost = Cost(
        Cost.COST_DISK_ACCESS_WRITE,
        Cost.COST_MEMORY_ACCESS
    ) * this.values.map { this.statistics[it.first].avgWidth }.sum() * this.input.outputSize

    /** The [UpdatePhysicalOperatorNode]s cannot be partitioned. */
    override val canBePartitioned: Boolean = false

    /**
     * Returns a copy of this [UpdatePhysicalOperatorNode] and its input.
     *
     * @return Copy of this [UpdatePhysicalOperatorNode] and its input.
     */
    override fun copyWithInputs() = UpdatePhysicalOperatorNode(this.input.copyWithInputs(), this.entity, this.values)

    /**
     * Returns a copy of this [UpdatePhysicalOperatorNode] and its output.
     *
     * @param inputs The [OperatorNode] that should act as inputs.
     * @return Copy of this [UpdatePhysicalOperatorNode] and its output.
     */
    override fun copyWithOutput(vararg inputs: OperatorNode.Physical): OperatorNode.Physical {
        require(inputs.size == 1) { "Only one input is allowed for unary operators." }
        val update = UpdatePhysicalOperatorNode(inputs[0], this.entity, this.values)
        return (this.output?.copyWithOutput(update) ?: update)
    }

    /**
     * Converts this [UpdatePhysicalOperatorNode] to a [UpdateOperator].
     *
     * @param tx The [TransactionContext] used for execution.
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(tx: TransactionContext, ctx: QueryContext): Operator {
        val entries = this.values.map { it.first to ctx.values[it.second] } /* Late binding. */
        return UpdateOperator(this.input.toOperator(tx, ctx), this.entity, entries)
    }

    /**
     * [InsertPhysicalOperatorNode] cannot be partitioned.
     */
    override fun partition(p: Int): List<Physical> {
        throw UnsupportedOperationException("UpdatePhysicalOperatorNode cannot be partitioned.")
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is UpdatePhysicalOperatorNode) return false

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