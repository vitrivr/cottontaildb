package org.vitrivr.cottontail.utilities.math.clustering

import org.vitrivr.cottontail.core.types.VectorValue

/**
 * A [Cluster], identified by a center and the points it contains.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface Cluster {
    /** The list of [VectorValue]s in this [Cluster]. */
    val points: List<VectorValue<*>>

    /** The centroid [VectorValue] of this [Cluster]. */
    val center: VectorValue<*>

    /**
     * Adds a [VectorValue] to this [Cluster].
     *
     * @param point The point [VectorValue] to add.
     */
    fun addPoint(point: VectorValue<*>)
}