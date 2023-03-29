package org.vitrivr.cottontail.dbms.queries.operators.logical.predicates

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.dbms.queries.operators.basics.BinaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.predicates.FilterOnSubSelectPhysicalOperatorNode

/**
 * A [BinaryLogicalOperatorNode] that formalizes filtering using some [BooleanPredicate].
 *
 * As opposed to [FilterLogicalOperatorNode], the [FilterOnSubSelectLogicalOperatorNode] depends on
 * the execution of one or many sub-queries.
 *
 * @author Ralph Gasser
 * @version 2.3.0
 */
class FilterOnSubSelectLogicalOperatorNode(val predicate: BooleanPredicate, left: Logical, right: Logical) : BinaryLogicalOperatorNode(left, right) {

    companion object {
        private const val NODE_NAME = "Filter"
    }

    /** The name of this [FilterOnSubSelectLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [FilterOnSubSelectLogicalOperatorNode] requires all [ColumnDef]s used in the [BooleanPredicate]. */
    override val requires: List<ColumnDef<*>> = this.predicate.columns.toList()

    /** The [FilterOnSubSelectLogicalOperatorNode] depends on all but the right input. */
    override val dependsOn: Array<GroupId> by lazy {
        arrayOf(this.right.groupId)
    }

    /**
     * Creates a copy of this [FilterOnSubSelectLogicalOperatorNode].
     *
     * @param input The new input [OperatorNode.Logical]
     * @return Copy of this [FilterOnSubSelectLogicalOperatorNode]
     */
    override fun copyWithNewInput(vararg input: Logical): FilterOnSubSelectLogicalOperatorNode {
        require(input.size == 2) { "The input arity for FilterOnSubSelectLogicalOperatorNode.copyWithNewInpu() must be 2 but is ${input.size}. This is a programmer's error!"}
        return FilterOnSubSelectLogicalOperatorNode(left = input[0], right = input[1], predicate = this.predicate)
    }

    /**
     * Returns a [FilterOnSubSelectPhysicalOperatorNode] representation of this [FilterOnSubSelectLogicalOperatorNode]
     *
     * @return [FilterOnSubSelectPhysicalOperatorNode]
     */
    override fun implement() = FilterOnSubSelectPhysicalOperatorNode(left = this.left.implement(), right = this.right.implement(), predicate = this.predicate)

    /** Generates and returns a [String] representation of this [FilterOnSubSelectLogicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.predicate}]"

    /**
     * Generates and returns a [Digest] for this [FilterOnSubSelectLogicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest = this.predicate.hashCode() + 2L
}