package ch.unibas.dmi.dbis.cottontail.math.knn.metrics

import ch.unibas.dmi.dbis.cottontail.model.values.*
import kotlin.math.sqrt

/**
 * Calculates the cosine distance, i.e. the angle, between two vectors.
 *
 * @version 1.0
 * @author Ralph Gasser
 */
object CosineDistance : DistanceKernel {
    override val cost: Double
        get() = 1.0

    /**
     * Calculates the L2 distance between two [VectorValue]s.
     *
     * @param a First [VectorValue]
     * @param b Second [VectorValue]
     *
     * @return Distance between a and b.
     */
    override operator fun invoke(a: VectorValue<*>, b: VectorValue<*>): Double {
        val dot = (a * b).sum()
        val c = a.pow(2).sum()
        val d = b.pow(2).sum()
        val div = sqrt(c) * sqrt(d)
        return if (div < 1e-6 || div.isNaN()) {
            1.0
        } else {
            1.0 - dot / div
        }
    }

    /**
     * Calculates the weighted L1 distance between two [VectorValue]s.
     *
     * @param a First [VectorValue]
     * @param b Second [VectorValue]
     * @param weights A list of weights
     *
     * @return Distance between a and b.
     */
    override operator fun invoke(a: VectorValue<*>, b: VectorValue<*>, weights: VectorValue<*>): Double {
        val dot = (a * b).timesInPlace(weights).sum()
        val c = a.pow(2).timesInPlace(weights).sum()
        val d = b.pow(2).timesInPlace(weights).sum()
        val div = sqrt(c) * sqrt(d)
        return if (div < 1e-6 || div.isNaN()) {
            1.0
        } else {
            1.0 - dot / div
        }
    }
}