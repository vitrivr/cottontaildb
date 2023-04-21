package org.vitrivr.cottontail.dbms.queries.operators.logical.sort

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.nodes.traits.OrderTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sort.SortPhysicalOperatorNode

/**
 * A [UnaryLogicalOperatorNode] that represents sorting the input by a set of specified [ColumnDef]s.
 *
 * @author Ralph Gasser
 * @version 2.3.0
 */
class SortLogicalOperatorNode(input: Logical, val sortOn: List<Pair<ColumnDef<*>, SortOrder>>) : UnaryLogicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Order"
    }

    /** The name of this [SortLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [SortLogicalOperatorNode] requires all [ColumnDef]s used in the [ProximityPredicate]. */
    override val requires: List<ColumnDef<*>> = this.sortOn.map { it.first }

    /** The [SortLogicalOperatorNode] overwrites/sets the [OrderTrait].  */
    override val traits: Map<TraitType<*>, Trait>
        get() = super.traits + listOf(OrderTrait to OrderTrait(this.sortOn))

    init {
        /* Sanity check. */
        if (this.sortOn.isEmpty()) throw QueryException.QuerySyntaxException("At least one column must be specified for sorting.")
    }

    /**
     * Creates a copy of this [SortLogicalOperatorNode].
     *
     * @param input The new input [OperatorNode.Logical]
     * @return Copy of this [SortLogicalOperatorNode]
     */
    override fun copyWithNewInput(vararg input: Logical): SortLogicalOperatorNode {
        require(input.size == 1) { "The input arity for SortLogicalOperatorNode.copyWithNewInput() must be 1 but is ${input.size}. This is a programmer's error!"}
        return SortLogicalOperatorNode(input = input[0], sortOn = this.sortOn)
    }

    /**
     * Returns a [SortPhysicalOperatorNode] representation of this [SortLogicalOperatorNode]
     *
     * @return [SortPhysicalOperatorNode]
     */
    override fun implement(): Physical = SortPhysicalOperatorNode(this.input.implement(), this.sortOn)

    /** Generates and returns a [String] representation of this [SortLogicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.sortOn.joinToString(",") { "${it.first.name} ${it.second}" }}]"

    /**
     * Generates and returns a [Digest] for this [SortLogicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest = this.sortOn.hashCode() + 3L
}