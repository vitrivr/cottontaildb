package org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.binding.Binding
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection.FunctionProjectionPhysicalOperatorNode
import org.vitrivr.cottontail.functions.basics.Function
import org.vitrivr.cottontail.model.basics.Name

/**
 * A [UnaryLogicalOperatorNode] that represents the execution of a [Function] to generate some [ColumnDef].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class FunctionProjectionLogicalOperatorNode(input: Logical? = null, val function: Function<*>, val arguments: List<Binding>, val alias: Name.ColumnName? = null) : UnaryLogicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Function"
    }

    /** The column produced by this [FunctionProjectionLogicalOperatorNode] is determined by the [Function]'s signature. */
    override val columns: List<ColumnDef<*>>
        get() = (this.input?.columns ?: emptyList()) + ColumnDef(
            name = this.alias ?: Name.ColumnName(this.function.signature.name.simple),
            type = this.function.signature.returnType!!,
            nullable = false
        )

    /** The [FunctionProjectionLogicalOperatorNode] requires all [ColumnDef] used in the [Function]. */
    override val requires: List<ColumnDef<*>>
        get() = this.arguments.filterIsInstance<Binding.Column>().map { it.column }

    /** [FunctionProjectionLogicalOperatorNode] can only be executed if [Function] can be executed. */
    override val executable: Boolean
        get() = super.executable && this.function.executable

    /** Human-readable name of this [FunctionProjectionLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /**
     * Creates and returns a copy of this [FunctionProjectionLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [FunctionProjectionLogicalOperatorNode].
     */
    override fun copy() = FunctionProjectionLogicalOperatorNode(function = this.function, arguments = this.arguments, alias = this.alias)

    /**
     * Returns a [FunctionProjectionPhysicalOperatorNode] representation of this [FunctionProjectionLogicalOperatorNode]
     *
     * @return [FunctionProjectionPhysicalOperatorNode]
     */
    override fun implement() = FunctionProjectionPhysicalOperatorNode(this.input?.implement(), this.function, this.arguments, this.alias)

    /**
     * Compares this [FunctionProjectionLogicalOperatorNode] to the given [Object] and returns true if they're equal and false otherwise.
     */
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is FunctionProjectionPhysicalOperatorNode) return false
        if (this.function != other.function) return false
        return true
    }

    /**
     * Generates and returns a hash code for this [FunctionProjectionLogicalOperatorNode].
     */
    override fun hashCode(): Int = this.function.hashCode()
}