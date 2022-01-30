package org.vitrivr.cottontail.dbms.queries.planning.nodes.physical.function

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.dbms.queries.QueryContext
import org.vitrivr.cottontail.dbms.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.function.FunctionOperator

/**
 * A [UnaryPhysicalOperatorNode] that represents the execution of a [Function] to generate some [ColumnDef].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class FunctionPhysicalOperatorNode(input: Physical? = null, val function: Function<*>, val out: Binding.Column, val arguments: List<Binding>) : UnaryPhysicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Function"
    }

    init {
        this.function.signature.arguments.forEachIndexed {  i, arg ->
            check(arg.type == this.arguments[i].type) { "Type ${this.arguments[i].type} of $i-th argument binding is incompatible with function ${function.signature}'s return type." }
        }
    }

    /** The column produced by this [FunctionPhysicalOperatorNode] is determined by the [Function]'s signature. */
    override val columns: List<ColumnDef<*>>
        get() = (this.input?.columns ?: emptyList()) + this.out.column

    /** The [FunctionPhysicalOperatorNode] requires all [ColumnDef] used in the [Function]. */
    override val requires: List<ColumnDef<*>>
        get() = this.arguments.filterIsInstance<Binding.Column>().map { it.column }

    /**The [FunctionPhysicalOperatorNode] cannot be partitioned. */
    override val canBePartitioned: Boolean = true

    /** The [Cost] of executing this [FunctionPhysicalOperatorNode]. */
    override val cost: Cost
        get() = Cost(cpu = this.outputSize * this.function.cost)

    /** [FunctionPhysicalOperatorNode] can only be executed if [Function] can be executed. */
    override val executable: Boolean
        get() = super.executable && this.function.executable

    /** Human-readable name of this [FunctionPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /**
     * Creates a copy of this [FunctionPhysicalOperatorNode].
     *
     * @return Copy of this [FunctionPhysicalOperatorNode]
     */
    override fun copy() = FunctionPhysicalOperatorNode(function = this.function.copy(), out = this.out.copy(), arguments = this.arguments.map { it.copy() })

    /**
     * Binds the provided [BindingContext] to this [Function], the output [Binding.Column] and the argument [Binding]s.
     *
     * @param context The new [BindingContext].
     */
    override fun bind(context: BindingContext) {
        super.bind(context)
        this.function.bind(context)
        this.arguments.forEach { it.bind(context) }
    }

    /**
     * Converts this [FunctionPhysicalOperatorNode] to a [FunctionOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator {
        val input = this.input?.toOperator(ctx) ?: throw IllegalStateException("Cannot convert disconnected OperatorNode to Operator (node = $this)")
        return FunctionOperator(input, this.function, this.out, this.arguments)
    }

    /**
     * Compares this [FunctionOperator] to the given [Object] and returns true if they're equal and false otherwise.
     */
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is FunctionPhysicalOperatorNode) return false
        if (this.function != other.function) return false
        if (this.out != other.out) return false
        return this.arguments != other.arguments
    }

    /**
     * Generates and returns a hash code for this [FunctionPhysicalOperatorNode].
     */
    override fun hashCode(): Int {
        var result = this.function.hashCode()
        result = 31 * result + this.out.hashCode()
        return 31 * result + this.arguments.hashCode()
    }

    /** Generates and returns a [String] representation of this [FunctionOperator]. */
    override fun toString() =  "${super.toString()}[${this.function.signature} -> ${this.out.column.name}]"
}