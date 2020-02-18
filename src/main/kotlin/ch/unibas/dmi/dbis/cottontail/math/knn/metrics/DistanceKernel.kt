package ch.unibas.dmi.dbis.cottontail.math.knn.metrics

import ch.unibas.dmi.dbis.cottontail.model.values.*

interface DistanceKernel {
    /** Estimate of the cost required per vector component. */
    val cost: Double

    /**
     * Calculates the distance between two [Complex32VectorValue]s.
     *
     * @param a First [Complex32VectorValue]
     * @param b Second [Complex32VectorValue]
     *
     * @return Distance between a and b.
     */
    operator fun invoke(a: VectorValue<*>, b: VectorValue<*>): Double

    /**
     * Calculates the distance between two [Complex32VectorValue]s.
     *
     * @param a First [Complex32VectorValue]
     * @param b Second [Complex32VectorValue]
     *
     * @return Distance between a and b.
     */
    operator fun invoke(a: VectorValue<*>, b: VectorValue<*>, weights: VectorValue<*>): Double
}