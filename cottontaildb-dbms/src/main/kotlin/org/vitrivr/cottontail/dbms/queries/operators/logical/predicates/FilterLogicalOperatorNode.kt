package org.vitrivr.cottontail.dbms.queries.operators.logical.predicates

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.predicates.FilterPhysicalOperatorNode

/**
 * A [UnaryLogicalOperatorNode] that formalizes filtering using some [BooleanPredicate].
 *
 * @author Ralph Gasser
 * @version 2.3.0
 */
class FilterLogicalOperatorNode(input: Logical, val predicate: BooleanPredicate) : UnaryLogicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Filter"
    }

    /** The name of this [FilterLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [FilterLogicalOperatorNode] requires all [ColumnDef]s used in the [BooleanPredicate]. */
    override val requires: List<ColumnDef<*>> = this.predicate.columns.toList()

    /**
     * Creates a copy of this [FilterLogicalOperatorNode].
     *
     * @param input The new input [OperatorNode.Logical]
     * @return Copy of this [FilterLogicalOperatorNode]
     */
    override fun copyWithNewInput(vararg input: Logical): FilterLogicalOperatorNode {
        require(input.size == 1) { "The input arity for UpdateLogicalOperatorNode.copyWithNewInput() must be 1 but is ${input.size}. This is a programmer's error!"}
        return FilterLogicalOperatorNode(input = input[0], predicate = this.predicate.copy())
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
        if (this.predicate != other.predicate) return false
        return true
    }

    /** Generates and returns a hash code for this [FilterLogicalOperatorNode]. */
    override fun hashCode(): Int = this.predicate.hashCode()

    /** Generates and returns a [String] representation of this [FilterLogicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.predicate}]"
}