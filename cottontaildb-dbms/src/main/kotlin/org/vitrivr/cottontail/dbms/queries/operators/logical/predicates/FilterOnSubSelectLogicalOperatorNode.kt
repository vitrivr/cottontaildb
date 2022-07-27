package org.vitrivr.cottontail.dbms.queries.operators.logical.predicates

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.dbms.queries.operators.basics.BinaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.NAryLogicalOperatorNode
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
class FilterOnSubSelectLogicalOperatorNode(val predicate: BooleanPredicate, vararg inputs: Logical) : NAryLogicalOperatorNode(*inputs) {

    companion object {
        private const val NODE_NAME = "Filter"
    }

    /** The name of this [FilterOnSubSelectLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [inputArity] of a [FilterOnSubSelectLogicalOperatorNode] depends on the [BooleanPredicate]. */
    override val inputArity: Int = this.inputs.size

    /** The [FilterOnSubSelectLogicalOperatorNode] requires all [ColumnDef]s used in the [BooleanPredicate]. */
    override val requires: List<ColumnDef<*>> = this.predicate.columns.toList()

    /** The [FilterOnSubSelectLogicalOperatorNode] inherits its traits from its left most input. */
    override val traits: Map<TraitType<*>, Trait>
        get() = super.inputs[0].traits

    /** The [FilterOnSubSelectLogicalOperatorNode] depends on all but the first [inputs]. */
    override val dependsOn: Array<GroupId> by lazy {
        this.inputs.drop(1).map { it.groupId }.toTypedArray()
    }

    /**
     * Creates a copy of this [FilterOnSubSelectLogicalOperatorNode].
     *
     * @param input The new input [OperatorNode.Logical]
     * @return Copy of this [FilterOnSubSelectLogicalOperatorNode]
     */
    override fun copyWithNewInput(vararg input: Logical): FilterOnSubSelectLogicalOperatorNode {
        return FilterOnSubSelectLogicalOperatorNode(inputs = input, predicate = this.predicate.copy())
    }

    /**
     * Returns a [FilterOnSubSelectPhysicalOperatorNode] representation of this [FilterOnSubSelectLogicalOperatorNode]
     *
     * @return [FilterOnSubSelectPhysicalOperatorNode]
     */
    override fun implement() = FilterOnSubSelectPhysicalOperatorNode(inputs = this.inputs.map { it.implement() }.toTypedArray(), predicate = this.predicate)

    /** Generates and returns a [String] representation of this [FilterOnSubSelectLogicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.predicate}]"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is FilterOnSubSelectLogicalOperatorNode) return false
        if (this.predicate != other.predicate) return false
        return true
    }

    override fun hashCode(): Int = this.predicate.hashCode()
}