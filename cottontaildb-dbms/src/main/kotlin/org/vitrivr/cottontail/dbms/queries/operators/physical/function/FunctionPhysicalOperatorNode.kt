package org.vitrivr.cottontail.dbms.queries.operators.physical.function

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.function.FunctionOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.physical.UnaryPhysicalOperatorNode

/**
 * A [UnaryPhysicalOperatorNode] that represents the execution of a [Function] to generate some [ColumnDef].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class FunctionPhysicalOperatorNode(input: Physical? = null, val function: Binding.Function, val out: Binding.Column) : UnaryPhysicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Function"
    }

    /** The column produced by this [FunctionPhysicalOperatorNode] is determined by the [Function]'s signature. */
    override val columns: List<ColumnDef<*>>
        get() = (this.input?.columns ?: emptyList()) + this.out.column

    /** The [FunctionPhysicalOperatorNode] requires all [ColumnDef] used in the [Function]. */
    override val requires: List<ColumnDef<*>>
        get() = this.function.requiredColumns()

    /** The [Cost] of a [FunctionPhysicalOperatorNode]. */
    override val cost: Cost
        get() = this.function.cost * this.outputSize

    /** [FunctionPhysicalOperatorNode] can only be executed if [Function] can be executed. */
    override val executable: Boolean
        get() = super.executable && this.function.function.executable

    /** Human-readable name of this [FunctionPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /**
     * Creates a copy of this [FunctionPhysicalOperatorNode].
     *
     * @return Copy of this [FunctionPhysicalOperatorNode]
     */
    override fun copy() = FunctionPhysicalOperatorNode(function = this.function.copy(), out = this.out.copy())

    /**
     * Converts this [FunctionPhysicalOperatorNode] to a [FunctionOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator {
        /* Bind relevant objects to binding context. */
        this.function.bind(ctx.bindings)
        this.out.bind(ctx.bindings)

        /* Convert input and append FunctionOperator. */
        val input = this.input?.toOperator(ctx) ?: throw IllegalStateException("Cannot convert disconnected OperatorNode to Operator (node = $this)")
        return FunctionOperator(input, this.function, this.out)
    }

    /**
     * Compares this [FunctionOperator] to the given [Object] and returns true if they're equal and false otherwise.
     */
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is FunctionPhysicalOperatorNode) return false
        if (this.function != other.function) return false
        if (this.out != other.out) return false
        return true
    }

    /**
     * Generates and returns a hash code for this [FunctionPhysicalOperatorNode].
     */
    override fun hashCode(): Int {
        val result = this.function.hashCode()
        return 31 * result + this.out.hashCode()
    }

    /** Generates and returns a [String] representation of this [FunctionOperator]. */
    override fun toString() =  "${super.toString()}[${this.function.function.signature} -> ${this.out.column.name}]"


}