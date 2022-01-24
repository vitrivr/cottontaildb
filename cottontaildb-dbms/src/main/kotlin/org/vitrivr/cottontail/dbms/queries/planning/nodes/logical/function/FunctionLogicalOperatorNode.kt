package org.vitrivr.cottontail.dbms.queries.planning.nodes.logical.function

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.functions.Function
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.dbms.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.nodes.logical.predicates.FilterLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.nodes.physical.function.FunctionPhysicalOperatorNode

/**
 * A [UnaryLogicalOperatorNode] that represents the execution of a [Function] that is manifested in an additional [ColumnDef].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class FunctionLogicalOperatorNode(input: Logical? = null, val function: Function<*>, val arguments: List<Binding>, val columnName: Name.ColumnName) : UnaryLogicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Function"
    }

    /** The column produced by this [FunctionLogicalOperatorNode] is determined by the [Function]'s signature. */
    override val columns: List<ColumnDef<*>>
        get() = (this.input?.columns ?: emptyList()) + ColumnDef(
            name = this.columnName,
            type = this.function.signature.returnType!!,
            nullable = false
        )

    /** The [FunctionLogicalOperatorNode] requires all [ColumnDef] used in the [Function]. */
    override val requires: List<ColumnDef<*>>
        get() = this.arguments.filterIsInstance<Binding.Column>().map { it.column }

    /** [FunctionLogicalOperatorNode] can only be executed if [Function] can be executed. */
    override val executable: Boolean
        get() = super.executable && this.function.executable

    /** Human-readable name of this [FunctionLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /**
     * Creates and returns a copy of this [FunctionLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [FunctionLogicalOperatorNode].
     */
    override fun copy() = FunctionLogicalOperatorNode(function = this.function, arguments = this.arguments, columnName = this.columnName)

    /**
     * Returns a [FunctionPhysicalOperatorNode] representation of this [FunctionLogicalOperatorNode]
     *
     * @return [FunctionPhysicalOperatorNode]
     */
    override fun implement() = FunctionPhysicalOperatorNode(this.input?.implement(), this.function, this.arguments, this.columnName)

    /**
     * Compares this [FunctionLogicalOperatorNode] to the given [Object] and returns true if they're equal and false otherwise.
     */
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is FunctionPhysicalOperatorNode) return false
        if (this.function != other.function) return false
        return true
    }

    /**
     * Generates and returns a hash code for this [FunctionLogicalOperatorNode].
     */
    override fun hashCode(): Int = this.function.hashCode()

    /** Generates and returns a [String] representation of this [FilterLogicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.function.signature} -> ${this.columnName}]"
}