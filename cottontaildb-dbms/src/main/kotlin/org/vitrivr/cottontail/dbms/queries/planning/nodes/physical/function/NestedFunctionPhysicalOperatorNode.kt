package org.vitrivr.cottontail.dbms.queries.planning.nodes.physical.function

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.functions.Function
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.dbms.queries.OperatorNode
import org.vitrivr.cottontail.dbms.queries.QueryContext
import org.vitrivr.cottontail.dbms.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.nodes.logical.function.NestedFunctionLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.function.FunctionOperator
import org.vitrivr.cottontail.execution.operators.function.NestedFunctionOperator

/**
 * A [UnaryLogicalOperatorNode] that represents the execution of a [Function] that is not manifested in an additional [ColumnDef], i.e., a nested function.
 *
 * Usually, [NestedFunctionLogicalOperatorNode] occur when other nodes that contain function calls are being evaluated.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class NestedFunctionPhysicalOperatorNode(input: Physical? = null, val function: Function<*>, val arguments: List<Binding>, val out: Binding.Literal) : UnaryPhysicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "NestedFunction"
    }

    /** The [NestedFunctionPhysicalOperatorNode] requires all [ColumnDef] used in the [Function]. */
    override val requires: List<ColumnDef<*>>
        get() = this.arguments.filterIsInstance<Binding.Column>().map { it.column }

    override fun copy(): UnaryPhysicalOperatorNode = NestedFunctionPhysicalOperatorNode(function = this.function, arguments =  this.arguments, out = this.out)

    /**The [NestedFunctionPhysicalOperatorNode] cannot be partitioned. */
    override val canBePartitioned: Boolean = true

    /** The [Cost] of executing this [FunctionPhysicalOperatorNode]. */
    override val cost: Cost
        get() = Cost(cpu = this.outputSize * this.function.cost)

    /** [NestedFunctionPhysicalOperatorNode] can only be executed if [Function] can be executed. */
    override val executable: Boolean
        get() = super.executable && this.function.executable

    /** Human-readable name of this [NestedFunctionPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /**
     * Converts this [NestedFunctionPhysicalOperatorNode] to a [FunctionOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator {
        val input = this.input?.toOperator(ctx) ?: throw IllegalStateException("Cannot convert disconnected OperatorNode to Operator (node = $this)")
        return NestedFunctionOperator(input, this.function, this.arguments, ctx.bindings, this.out)
    }

    /**
     * Partitions this [NestedFunctionPhysicalOperatorNode].
     *
     * @param p The number of partitions to create.
     * @return List of [OperatorNode.Physical], each representing a partition of the original tree.
     */
    override fun partition(p: Int): List<Physical> {
        val input = this.input ?: throw IllegalStateException("Cannot partition disconnected OperatorNode (node = $this)")
        return input.partition(p).map { NestedFunctionPhysicalOperatorNode(it, this.function, this.arguments, this.out) }
    }

    /**
     * Compares this [NestedFunctionPhysicalOperatorNode] to the given [Object] and returns true if they're equal and false otherwise.
     */
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is NestedFunctionPhysicalOperatorNode) return false
        if (this.function != other.function) return false
        if (this.out != other.out) return false
        return true
    }

    /**
     * Generates and returns a hash code for this [NestedFunctionPhysicalOperatorNode].
     */
    override fun hashCode(): Int {
        var result = this.function.hashCode()
        result = 31 * result + this.out.hashCode()
        return result
    }

    /** Generates and returns a [String] representation of this [NestedFunctionPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.function.signature}]"
}