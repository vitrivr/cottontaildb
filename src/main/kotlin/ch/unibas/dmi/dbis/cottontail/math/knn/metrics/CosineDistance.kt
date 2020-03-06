package ch.unibas.dmi.dbis.cottontail.math.knn.metrics

import ch.unibas.dmi.dbis.cottontail.model.values.types.VectorValue
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
    override operator fun invoke(a: VectorValue<*>, b: VectorValue<*>): Double = ((a dot b) / (a.norm2() * b.norm2())).value.toDouble()

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
        val wa = (a * weights)
        val wb = (b * weights)
        return ((wa dot wb) / (wa.norm2() * wb.norm2())).value.toDouble()
    }
}