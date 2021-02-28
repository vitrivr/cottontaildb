package org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.predicates.FilterPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate

/**
 * A [UnaryLogicalOperatorNode] that formalizes filtering using some [BooleanPredicate].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class FilterLogicalOperatorNode(input: OperatorNode.Logical, val predicate: BooleanPredicate) : UnaryLogicalOperatorNode(input) {

    /** The [FilterLogicalOperatorNode] returns the [ColumnDef] of its input, or no column at all. */
    override val columns: Array<ColumnDef<*>>
        get() = this.input.columns

    /** The [FilterLogicalOperatorNode] requires all [ColumnDef]s used in the [BooleanPredicate]. */
    override val requires: Array<ColumnDef<*>>
        get() = this.predicate.columns.toTypedArray()

    /**
     * Returns a copy of this [FilterLogicalOperatorNode] and its input.
     *
     * @return Copy of this [FilterLogicalOperatorNode] and its input.
     */
    override fun copyWithInputs() = FilterLogicalOperatorNode(this.input.copyWithInputs(), this.predicate)

    /**
     * Returns a copy of this [FilterLogicalOperatorNode] and its output.
     *
     * @param input The [OperatorNode.Logical] that should act as inputs.
     * @return Copy of this [FilterLogicalOperatorNode] and its output.
     */
    override fun copyWithOutput(input: OperatorNode.Logical?): OperatorNode.Logical {
        require(input != null) { "Input is required for unary logical operator node." }
        val filter = FilterLogicalOperatorNode(input, this.predicate)
        return (this.output?.copyWithOutput(filter) ?: filter)
    }

    /**
     * Returns a [FilterPhysicalOperatorNode] representation of this [FilterLogicalOperatorNode]
     *
     * @return [FilterPhysicalOperatorNode]
     */
    override fun implement(): Physical = FilterPhysicalOperatorNode(this.input.implement(), this.predicate)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is FilterLogicalOperatorNode) return false
        if (predicate != other.predicate) return false
        return true
    }

    override fun hashCode(): Int = this.predicate.hashCode()
}