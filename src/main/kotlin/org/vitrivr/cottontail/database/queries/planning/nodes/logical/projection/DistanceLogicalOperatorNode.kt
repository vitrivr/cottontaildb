package org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection.DistancePhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.utilities.math.KnnUtilities

/**
 * A [UnaryLogicalOperatorNode] that represents calculating the distance of a certain [ColumnDef] to a certain
 * query vector (expressed by a [KnnPredicate]).
 *
 * This can be used for late population, which can lead to optimized performance for kNN queries
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class DistanceLogicalOperatorNode(input: OperatorNode.Logical, val predicate: KnnPredicate) : UnaryLogicalOperatorNode(input) {

    /** The [DistanceLogicalOperatorNode] returns the [ColumnDef] of its input + a distance column. */
    override val columns: Array<ColumnDef<*>> = this.input.columns + KnnUtilities.distanceColumnDef(this.predicate.column.name.entity())

    /** The [DistanceLogicalOperatorNode] requires all [ColumnDef]s used in the [KnnPredicate]. */
    override val requires: Array<ColumnDef<*>> = arrayOf(this.predicate.column)

    /**
     * Returns a copy of this [DistanceLogicalOperatorNode] and its input.
     *
     * @return Copy of this [DistanceLogicalOperatorNode] and its input.
     */
    override fun copyWithInputs(): DistanceLogicalOperatorNode = DistanceLogicalOperatorNode(this.input.copyWithInputs(), this.predicate)

    /**
     * Returns a copy of this [DistanceLogicalOperatorNode] and its output.
     *
     * @param input The [OperatorNode.Logical] that should act as inputs.
     * @return Copy of this [DistanceLogicalOperatorNode] and its output.
     */
    override fun copyWithOutput(input: OperatorNode.Logical?): OperatorNode.Logical {
        require(input != null) { "Input is required for unary logical operator node." }
        val distance = DistanceLogicalOperatorNode(input, this.predicate)
        return (this.output?.copyWithOutput(distance) ?: distance)
    }

    /**
     * Returns a [DistancePhysicalOperatorNode] representation of this [DistanceLogicalOperatorNode]
     *
     * @return [DistancePhysicalOperatorNode]
     */
    override fun implement(): Physical = DistancePhysicalOperatorNode(this.input.implement(), this.predicate)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is DistanceLogicalOperatorNode) return false

        if (predicate != other.predicate) return false

        return true
    }

    override fun hashCode(): Int = this.predicate.hashCode()
}