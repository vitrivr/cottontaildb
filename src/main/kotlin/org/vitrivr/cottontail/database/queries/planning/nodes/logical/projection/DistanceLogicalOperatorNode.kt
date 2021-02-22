package org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection

import org.vitrivr.cottontail.database.column.ColumnDef
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
 * @version 1.0.0
 */
class DistanceLogicalOperatorNode(val predicate: KnnPredicate) : UnaryLogicalOperatorNode() {

    /** The [DistanceLogicalOperatorNode] returns the [ColumnDef] of its input + a distance column. */
    override val columns: Array<ColumnDef<*>>
        get() = this.input.columns + KnnUtilities.distanceColumnDef(this.predicate.column.name.entity())

    /** The [DistanceLogicalOperatorNode] requires all [ColumnDef]s used in the [KnnPredicate]. */
    override val requires: Array<ColumnDef<*>>
        get() = (super.requires + this.predicate.columns)

    /**
     * Returns a copy of this [DistanceLogicalOperatorNode]
     *
     * @return Copy of this [DistanceLogicalOperatorNode]
     */
    override fun copy(): DistanceLogicalOperatorNode = DistanceLogicalOperatorNode(this.predicate)

    /**
     * Returns a [DistancePhysicalOperatorNode] representation of this [DistanceLogicalOperatorNode]
     *
     * @return [DistancePhysicalOperatorNode]
     */
    override fun implement(): Physical = DistancePhysicalOperatorNode(this.predicate)

    /**
     * Calculates and returns the digest for this [DistanceLogicalOperatorNode].
     *
     * @return Digest for this [DistanceLogicalOperatorNode]
     */
    override fun digest(): Long = 31L * super.digest() + this.predicate.digest()
}