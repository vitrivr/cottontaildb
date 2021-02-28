package org.vitrivr.cottontail.database.queries.planning.nodes.logical.sort

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.sort.SortPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.model.exceptions.QueryException

/**
 * A [UnaryLogicalOperatorNode] that represents sorting the input by a set of specified [ColumnDef]s.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class SortLogicalOperatorNode(input: OperatorNode.Logical, sortOn: Array<Pair<ColumnDef<*>, SortOrder>>) : UnaryLogicalOperatorNode(input) {
    init {
        /* Sanity check. */
        if (sortOn.isEmpty()) throw QueryException.QuerySyntaxException("At least one column must be specified for sorting.")
    }

    /** The [SortLogicalOperatorNode] returns the [ColumnDef] of its input. */
    override val columns: Array<ColumnDef<*>> = this.input.columns

    /** A [SortLogicalOperatorNode] orders the input in by the specified [ColumnDef]s. */
    override val order = sortOn

    /** The [SortLogicalOperatorNode] requires all [ColumnDef]s used in the [KnnPredicate]. */
    override val requires: Array<ColumnDef<*>> = this.order.map { it.first }.toTypedArray()

    /**
     * Copies this [SortLogicalOperatorNode] and its input.
     *
     * @return Copy of this [SortLogicalOperatorNode] and its input.
     */
    override fun copyWithInputs() = SortLogicalOperatorNode(input.copyWithInputs(), this.order)

    /**
     * Returns a copy of this [SortLogicalOperatorNode] and its output.
     *
     * @param input The [OperatorNode.Logical] that should act as inputs.
     * @return Copy of this [SortLogicalOperatorNode] and its output.
     */
    override fun copyWithOutput(input: OperatorNode.Logical?): OperatorNode.Logical {
        require(input != null) { "Input is required for unary logical operator node." }
        val sort = SortLogicalOperatorNode(input, this.order)
        return (this.output?.copyWithOutput(sort) ?: sort)
    }

    /**
     * Returns a [SortPhysicalOperatorNode] representation of this [SortLogicalOperatorNode]
     *
     * @return [SortPhysicalOperatorNode]
     */
    override fun implement(): Physical = SortPhysicalOperatorNode(this.input.implement(), this.order)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is SortLogicalOperatorNode) return false

        if (!order.contentEquals(other.order)) return false

        return true
    }

    override fun hashCode(): Int = this.order.contentHashCode()
}