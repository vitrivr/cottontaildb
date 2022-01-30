package org.vitrivr.cottontail.dbms.queries.planning.nodes.physical.function

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
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
class NestedFunctionPhysicalOperatorNode(input: Physical? = null, val function: Function<*>, val out: Binding.Literal, val arguments: List<Binding>) : UnaryPhysicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "NestedFunction"
    }

    init {
        require(this.function.signature.returnType == this.out.type) { "Type ${out.type} of output binding is incompatible with function ${function.signature}'s return type." }
        this.function.signature.arguments.forEachIndexed {  i, arg ->
            check(arg.type == this.arguments[i].type) { "Type ${this.arguments[i].type} of $i-th argument binding is incompatible with function ${function.signature}'s return type." }
        }
    }

    /** The [NestedFunctionPhysicalOperatorNode] requires all [ColumnDef] used in the [Function]. */
    override val requires: List<ColumnDef<*>>
        get() = this.arguments.filterIsInstance<Binding.Column>().map { it.column }

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
     * Creates a copy of this [FunctionPhysicalOperatorNode].
     *
     * @return Copy of this [FunctionPhysicalOperatorNode]
     */
    override fun copy(): UnaryPhysicalOperatorNode = NestedFunctionPhysicalOperatorNode(function = this.function.copy(), out = this.out.copy(), arguments = this.arguments.map { it.copy() })

    /**
     * Binds the provided [BindingContext] to this [Function], the output [Binding.Column] and the argument [Binding]s.
     *
     * @param context The new [BindingContext].
     */
    override fun bind(context: BindingContext) {
        super.bind(context)
        this.function.bind(context)
        this.out.bind(context)
        this.arguments.forEach { it.bind(context) }
    }

    /**
     * Converts this [NestedFunctionPhysicalOperatorNode] to a [FunctionOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator {
        val input = this.input?.toOperator(ctx) ?: throw IllegalStateException("Cannot convert disconnected OperatorNode to Operator (node = $this)")
        return NestedFunctionOperator(input, this.function, this.out, this.arguments)
    }

    /**
     * Compares this [NestedFunctionPhysicalOperatorNode] to the given [Object] and returns true if they're equal and false otherwise.
     */
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is NestedFunctionPhysicalOperatorNode) return false
        if (this.function != other.function) return false
        return true
    }

    /**
     * Generates and returns a hash code for this [NestedFunctionPhysicalOperatorNode].
     */
    override fun hashCode(): Int = 123 * this.function.hashCode()

    /** Generates and returns a [String] representation of this [NestedFunctionPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.function.signature}]"
}