package org.vitrivr.cottontail.dbms.queries.planning.nodes.physical.function

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.dbms.queries.OperatorNode
import org.vitrivr.cottontail.dbms.queries.QueryContext
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.dbms.queries.planning.nodes.logical.function.FunctionLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.function.FunctionOperator
import org.vitrivr.cottontail.core.functions.Function
import org.vitrivr.cottontail.core.database.Name

/**
 * A [UnaryPhysicalOperatorNode] that represents the execution of a [Function] to generate some [ColumnDef].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class FunctionPhysicalOperatorNode(input: Physical? = null, val function: Function<*>, val arguments: List<Binding>, val alias: Name.ColumnName? = null) : UnaryPhysicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Function"
    }

    /** The [ColumnDef] being produced by this [FunctionPhysicalOperatorNode]. */
    val produces: ColumnDef<*> = ColumnDef(
        name = this.alias ?: Name.ColumnName(this.function.signature.name.simple),
        type = this.function.signature.returnType!!,
        nullable = false
    )

    /** The column produced by this [FunctionPhysicalOperatorNode] is determined by the [Function]'s signature. */
    override val columns: List<ColumnDef<*>>
        get() = (this.input?.columns ?: emptyList()) + this.produces

    /** The [FunctionPhysicalOperatorNode] requires all [ColumnDef] used in the [Function]. */
    override val requires: List<ColumnDef<*>>
        get() = this.arguments.filterIsInstance<Binding.Column>().map { it.column }

    override fun copy(): UnaryPhysicalOperatorNode = FunctionPhysicalOperatorNode(function = this.function, arguments =  this.arguments, alias = this.alias)

    /**The [ExistsProjectionPhysicalOperatorNode] cannot be partitioned. */
    override val canBePartitioned: Boolean = true

    /** The [Cost] of executing this [FunctionPhysicalOperatorNode]. */
    override val cost: Cost
        get() = Cost(cpu = this.outputSize * this.function.cost)

    /** [FunctionLogicalOperatorNode] can only be executed if [Function] can be executed. */
    override val executable: Boolean
        get() = super.executable && this.function.executable

    /** Human-readable name of this [FunctionLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /**
     * Converts this [FunctionPhysicalOperatorNode] to a [FunctionOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator {
        val input = this.input?.toOperator(ctx) ?: throw IllegalStateException("Cannot convert disconnected OperatorNode to Operator (node = $this)")
        return FunctionOperator(input, this.function, this.arguments, ctx.bindings, this.alias)
    }

    /**
     * Partitions this [FunctionPhysicalOperatorNode].
     *
     * @param p The number of partitions to create.
     * @return List of [OperatorNode.Physical], each representing a partition of the original tree.
     */
    override fun partition(p: Int): List<Physical> {
        val input = this.input ?: throw IllegalStateException("Cannot partition disconnected OperatorNode (node = $this)")
        return input.partition(p).map { FunctionPhysicalOperatorNode(it, this.function, this.arguments, this.alias) }
    }

    /** Generates and returns a [String] representation of this [FunctionOperator]. */
    override fun toString() = if (this.alias != null) {
        "${super.toString()}[${this.function.signature} -> ${this.alias}]"
    } else {
        "${super.toString()}[${this.function.signature}]"
    }

    /**
     * Compares this [FunctionOperator] to the given [Object] and returns true if they're equal and false otherwise.
     */
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is FunctionPhysicalOperatorNode) return false
        if (this.function != other.function) return false
        if (this.alias != other.alias) return false
        return true
    }

    /**
     * Generates and returns a hash code for this [FunctionPhysicalOperatorNode].
     */
    override fun hashCode(): Int {
        var result = this.function.hashCode()
        result = 31 * result + alias.hashCode()
        return result
    }
}