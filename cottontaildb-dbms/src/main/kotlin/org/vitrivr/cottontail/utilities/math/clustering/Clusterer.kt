package org.vitrivr.cottontail.utilities.math.clustering

import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.values.types.VectorValue

/**
 * A class that can be used to cluster a list of [VectorValue]s into a defined number of [Cluster]s
 * using a specified [VectorDistance]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface Clusterer {

    /** The [VectorDistance] used by this [Cluster]. */
    val distance: VectorDistance<*>

    /**
     * Clusters a [List] of points [VectorValue] with this [Clusterer].
     *
     * @param points The [List] of [VectorValue]s to cluster.
     * @return List of resulting [Cluster]s.
     */
    fun cluster(points: List<VectorValue<*>>): List<Cluster>
}