package org.vitrivr.cottontail.dbms.queries.operators.logical.sort

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.dbms.queries.operators.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.sort.SortPhysicalOperatorNode
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.dbms.queries.sort.SortOrder
import org.vitrivr.cottontail.dbms.exceptions.QueryException

/**
 * A [UnaryLogicalOperatorNode] that represents sorting the input by a set of specified [ColumnDef]s.
 *
 * @author Ralph Gasser
 * @version 2.2.0
 */
class SortLogicalOperatorNode(input: Logical? = null, override val sortOn: List<Pair<ColumnDef<*>, SortOrder>>) : UnaryLogicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Order"
    }

    /** The name of this [SortLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [SortLogicalOperatorNode] requires all [ColumnDef]s used in the [ProximityPredicate]. */
    override val requires: List<ColumnDef<*>> = this.sortOn.map { it.first }

    init {
        /* Sanity check. */
        if (this.sortOn.isEmpty()) throw QueryException.QuerySyntaxException("At least one column must be specified for sorting.")
    }

    /**
     * Creates and returns a copy of this [SortLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [SortLogicalOperatorNode].
     */
    override fun copy() = SortLogicalOperatorNode(sortOn = this.sortOn)

    /**
     * Returns a [SortPhysicalOperatorNode] representation of this [SortLogicalOperatorNode]
     *
     * @return [SortPhysicalOperatorNode]
     */
    override fun implement(): Physical = SortPhysicalOperatorNode(this.input?.implement(), this.sortOn)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is SortLogicalOperatorNode) return false
        if (this.sortOn != other.sortOn) return false
        return true
    }

    /** Generates and returns a hash code for this [SortLogicalOperatorNode]. */
    override fun hashCode(): Int = this.sortOn.hashCode()

    /** Generates and returns a [String] representation of this [SortLogicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.sortOn.joinToString(",") { "${it.first.name} ${it.second}" }}]"
}