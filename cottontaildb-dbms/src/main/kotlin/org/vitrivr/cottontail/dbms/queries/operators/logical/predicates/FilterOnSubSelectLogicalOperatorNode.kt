package org.vitrivr.cottontail.dbms.queries.operators.logical.predicates

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.dbms.queries.operators.logical.BinaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.NAryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.predicates.FilterOnSubSelectPhysicalOperatorNode

/**
 * A [BinaryLogicalOperatorNode] that formalizes filtering using some [BooleanPredicate].
 *
 * As opposed to [FilterLogicalOperatorNode], the [FilterOnSubSelectLogicalOperatorNode] depends on
 * the execution of one or many sub-queries.
 *
 * @author Ralph Gasser
 * @version 2.2.0
 */
class FilterOnSubSelectLogicalOperatorNode(val predicate: BooleanPredicate, vararg inputs: Logical) : NAryLogicalOperatorNode(*inputs) {

    companion object {
        private const val NODE_NAME = "Filter"
    }

    /** The name of this [FilterOnSubSelectLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [inputArity] of a [FilterOnSubSelectLogicalOperatorNode] depends on the [BooleanPredicate]. */
    override val inputArity: Int = this.predicate.atomics.count { it.dependsOn != -1 } + 1

    /** The [FilterOnSubSelectLogicalOperatorNode] requires all [ColumnDef]s used in the [BooleanPredicate]. */
    override val requires: List<ColumnDef<*>> = this.predicate.columns.toList()

    /** The [FilterOnSubSelectLogicalOperatorNode] inherits its traits from its left most input. */
    override val traits: Map<TraitType<*>, Trait>
        get() = super.inputs[0].traits

    /**
     * Creates and returns a copy of this [FilterOnSubSelectLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [FilterOnSubSelectLogicalOperatorNode].
     */
    override fun copy() = FilterOnSubSelectLogicalOperatorNode(predicate = this.predicate)

    /**
     * Returns a [FilterOnSubSelectPhysicalOperatorNode] representation of this [FilterOnSubSelectLogicalOperatorNode]
     *
     * @return [FilterOnSubSelectPhysicalOperatorNode]
     */
    override fun implement() = FilterOnSubSelectPhysicalOperatorNode(this.predicate, *this.inputs.map { it.implement() }.toTypedArray())

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