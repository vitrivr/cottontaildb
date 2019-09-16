package ch.unibas.dmi.dbis.cottontail.math.knn.metrics

import ch.unibas.dmi.dbis.cottontail.model.values.VectorValue
import java.util.*


/**
 * Interface implemented by [DistanceFunction]s that can be used to calculate the distance between two vectors.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
interface DistanceFunction<T> {
    /**
     * Estimation of the number of operations required per vector component.
     */
    val operations: Int

    /**
     * Calculates the weighted distance between two [BitSet]s, i.e. [BooleanArray]'s where
     * each element can either be 1 or 0.
     *
     * @param a First [BitSet]
     * @param b Second [BitSet]
     * @param weights The [Float] containing the weights.
     *
     * @return Distance between a and b.
     */
    operator fun invoke(a: VectorValue<T>, b: VectorValue<T>): Double

    /**
     * Calculates the weighted distance between two [FloatArray]s
     *
     * @param a First [FloatArray]
     * @param b Second [FloatArray]
     * @param weights The [FloatArray] containing the weights.
     *
     * @return Distance between a and b.
     */
    operator fun invoke(a: VectorValue<T>, b: VectorValue<T>, weights: VectorValue<*>): Double
}