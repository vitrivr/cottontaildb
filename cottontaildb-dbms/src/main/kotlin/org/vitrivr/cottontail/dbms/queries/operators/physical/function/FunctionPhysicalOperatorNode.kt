package org.vitrivr.cottontail.dbms.queries.operators.physical.function

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.function.FunctionOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.UnaryPhysicalOperatorNode

/**
 * A [UnaryPhysicalOperatorNode] that represents the execution of a [Function] to generate some [ColumnDef].
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class FunctionPhysicalOperatorNode(input: Physical, val function: Binding.Function, val out: Binding.Column) : UnaryPhysicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Function"
    }

    /** The column produced by this [FunctionPhysicalOperatorNode] is determined by the [Function]'s signature. */
    override val columns: List<Binding.Column> by lazy {
        this.input.columns + this.out
    }

    /** The [FunctionPhysicalOperatorNode] requires all [ColumnDef] used in the [Function]. */
    override val requires: List<Binding.Column> by lazy {
        this.function.requiredColumns()
    }

    /** The [Cost] of a [FunctionPhysicalOperatorNode]. */
    context(BindingContext, Tuple)    override val cost: Cost
        get() = this.function.cost * this.outputSize

    /** Human-readable name of this [FunctionPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /**
     * Creates a copy of this [FunctionPhysicalOperatorNode].
     *
     * @param input The new input [OperatorNode.Physical]
     * @return Copy of this [FunctionPhysicalOperatorNode]
     */
    override fun copyWithNewInput(vararg input: Physical): UnaryPhysicalOperatorNode {
        require(input.size == 1) { "The input arity for FunctionPhysicalOperatorNode.copyWithNewInput() must be 1 but is ${input.size}. This is a programmer's error!"}
        return FunctionPhysicalOperatorNode(input = input[0], function = this.function.copy(), out = this.out.copy())
    }

    /**
     * Converts this [FunctionPhysicalOperatorNode] to a [FunctionOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator = FunctionOperator(this.input.toOperator(ctx), this.function, this.out, ctx)

    /**
     * Generates and returns a [Digest] for this [FunctionPhysicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest {
        val result = this.function.hashCode().toLong()
        return 31L * result + this.out.hashCode()
    }

    /** Generates and returns a [String] representation of this [FunctionOperator]. */
    override fun toString() =  "${super.toString()}[${this.function.function.signature} -> ${this.out.column.name}]"
}