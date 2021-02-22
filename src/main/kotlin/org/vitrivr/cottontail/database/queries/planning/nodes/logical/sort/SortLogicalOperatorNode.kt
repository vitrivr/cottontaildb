package org.vitrivr.cottontail.database.queries.planning.nodes.logical.sort

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sort.SortPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.model.exceptions.QueryException

/**
 * A [UnaryLogicalOperatorNode] that represents sorting the input by a set of specified [ColumnDef]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class SortLogicalOperatorNode(sortOn: Array<Pair<ColumnDef<*>, SortOrder>>) : UnaryLogicalOperatorNode() {
    init {
        /* Sanity check. */
        if (sortOn.isEmpty()) throw QueryException.QuerySyntaxException("At least one column must be specified for sorting.")
    }

    /** The [SortLogicalOperatorNode] returns the [ColumnDef] of its input. */
    override val columns: Array<ColumnDef<*>>
        get() = this.input.columns

    /** The [SortLogicalOperatorNode] requires all [ColumnDef]s used in the [KnnPredicate]. */
    override val requires: Array<ColumnDef<*>>
        get() = super.requires + this.order.map { it.first }.toTypedArray()

    /** A [SortLogicalOperatorNode] orders the input in by the specified [ColumnDef]s. */
    override val order = sortOn

    /** Copies this [SortLogicalOperatorNode]. */
    override fun copy() = SortLogicalOperatorNode(this.order)

    /**
     * Returns a [SortPhysicalOperatorNode] representation of this [SortLogicalOperatorNode]
     *
     * @return [SortPhysicalOperatorNode]
     */
    override fun implement(): Physical = SortPhysicalOperatorNode(this.order)
}