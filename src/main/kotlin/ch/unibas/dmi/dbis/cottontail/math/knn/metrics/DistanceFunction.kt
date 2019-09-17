package ch.unibas.dmi.dbis.cottontail.math.knn.metrics

import ch.unibas.dmi.dbis.cottontail.model.values.VectorValue

/**
 * Interface implemented by [DistanceFunction]s that can be used to calculate the distance between two vectors.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
interface DistanceFunction<T> {
    /**
     * Estimation of the number of operations required per vector component.
     */
    val operations: Int

    /**
     * Calculates the weighted distance between two [VectorValue].
     *
     * @param a First [VectorValue]
     * @param b Second [VectorValue]
     *
     * @return Distance between a and b.
     */
    operator fun invoke(a: VectorValue<T>, b: VectorValue<T>): Double

    /**
     * Calculates the weighted distance between two [VectorValue]s
     *
     * @param a First [VectorValue]
     * @param b Second [VectorValue]
     * @param weights The [VectorValue] containing the weights.
     *
     * @return Distance between a and b.
     */
    operator fun invoke(a: VectorValue<T>, b: VectorValue<T>, weights: VectorValue<*>): Double
}