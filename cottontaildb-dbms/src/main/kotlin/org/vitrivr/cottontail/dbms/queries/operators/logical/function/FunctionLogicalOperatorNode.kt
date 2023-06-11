package org.vitrivr.cottontail.dbms.queries.operators.logical.function

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.functions.Function
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.predicates.FilterLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.function.FunctionPhysicalOperatorNode

/**
 * A [UnaryLogicalOperatorNode] that represents the execution of a [Function] that is manifested in an additional [ColumnDef].
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class FunctionLogicalOperatorNode(input: Logical, val function: Binding.Function, val out: Binding.Column) : UnaryLogicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Function"
    }

    /** The column produced by this [FunctionLogicalOperatorNode] is determined by the [Function]'s signature. */
    override val columns: List<ColumnDef<*>> by lazy {
        this.input.columns + this.out.column
    }

    /** The [FunctionLogicalOperatorNode] requires all [ColumnDef] used in the [Function]. */
    override val requires: List<ColumnDef<*>>
        get() = this.function.requiredColumns()

    /** Human-readable name of this [FunctionLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /**
     * Creates a copy of this [FunctionLogicalOperatorNode].
     *
     * @param input The new input [OperatorNode.Logical]
     * @return Copy of this [FunctionLogicalOperatorNode]
     */
    override fun copyWithNewInput(vararg input: Logical): FunctionLogicalOperatorNode {
        require(input.size == 1) { "The input arity for FunctionLogicalOperatorNode.copyWithNewInput() must be 1 but is ${input.size}. This is a programmer's error!"}
        return FunctionLogicalOperatorNode(input = input[0], function = this.function.copy(), out = this.out.copy())
    }

    /**
     * Returns a [FunctionPhysicalOperatorNode] representation of this [FunctionLogicalOperatorNode]
     *
     * @return [FunctionPhysicalOperatorNode]
     */
    override fun implement() = FunctionPhysicalOperatorNode(this.input.implement(), this.function, this.out)

    /**
     * Generates and returns a [Digest] for this [FunctionPhysicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest {
        val result = this.function.hashCode().toLong()
        return 31L * result + this.out.hashCode()
    }

    /** Generates and returns a [String] representation of this [FilterLogicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.function.function.signature} -> ${this.out.column.name}]"
}