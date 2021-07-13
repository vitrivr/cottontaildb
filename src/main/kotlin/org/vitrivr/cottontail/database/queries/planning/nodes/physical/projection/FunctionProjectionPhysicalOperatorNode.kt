package org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.Binding
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.projection.DistanceProjectionOperator
import org.vitrivr.cottontail.execution.operators.projection.FunctionProjectionOperator
import org.vitrivr.cottontail.functions.basics.Function
import org.vitrivr.cottontail.model.basics.Name

/**
 * A [UnaryPhysicalOperatorNode] that represents the execution of a [Function] to generate some [ColumnDef].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class FunctionProjectionPhysicalOperatorNode(input: Physical? = null, val function: Function<*>, val arguments: List<Binding>, val alias: Name.ColumnName? = null) : UnaryPhysicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Function"
    }

    /** The column produced by this [FunctionProjectionPhysicalOperatorNode] is determined by the [Function]'s signature. */
    override val columns: List<ColumnDef<*>>
        get() = (this.input?.columns ?: emptyList()) + ColumnDef(this.alias ?: Name.ColumnName(this.function.signature.name), this.function.signature.returnType!!)

    /** The [FunctionProjectionPhysicalOperatorNode] requires all [ColumnDef] used in the [Function]. */
    override val requires: List<ColumnDef<*>>
        get() = this.arguments.filterIsInstance<Binding.Column>().map { it.column }

    override fun copy(): UnaryPhysicalOperatorNode = FunctionProjectionPhysicalOperatorNode(function = this.function, arguments =  this.arguments, alias = this.alias)

    /**The [ExistsProjectionPhysicalOperatorNode] cannot be partitioned. */
    override val canBePartitioned: Boolean = true

    /** The [Cost] of executing this [FunctionProjectionPhysicalOperatorNode]. */
    override val cost: Cost
        get() = Cost(cpu = this.outputSize * this.function.cost)

    override val name: String
        get() = NODE_NAME

    /**
     * Converts this [FunctionProjectionPhysicalOperatorNode] to a [DistanceProjectionOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator {
        val input = this.input?.toOperator(ctx) ?: throw IllegalStateException("Cannot convert disconnected OperatorNode to Operator (node = $this)")
        return FunctionProjectionOperator(input, this.function, this.arguments, this.alias)
    }

    /**
     * Partitions this [FunctionProjectionPhysicalOperatorNode].
     *
     * @param p The number of partitions to create.
     * @return List of [OperatorNode.Physical], each representing a partition of the original tree.
     */
    override fun partition(p: Int): List<Physical> {
        val input = this.input ?: throw IllegalStateException("Cannot partition disconnected OperatorNode (node = $this)")
        return input.partition(p).map { FunctionProjectionPhysicalOperatorNode(it, this.function, this.arguments, this.alias) }
    }

    /**
     * Compares this [FunctionProjectionOperator] to the given [Object] and returns true if they're equal and false otherwise.
     */
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is FunctionProjectionPhysicalOperatorNode) return false

        if (this.function != other.function) return false
        if (this.alias != other.alias) return false

        return true
    }

    /**
     * Generates and returns a hash code for this [FunctionProjectionPhysicalOperatorNode].
     */
    override fun hashCode(): Int {
        var result = this.function.hashCode()
        result = 31 * result + alias.hashCode()
        return result
    }
}