package ch.unibas.dmi.dbis.cottontail.math.knn.metrics

import ch.unibas.dmi.dbis.cottontail.model.values.VectorValue

/**
 * Interface implemented by [DistanceFunction]s that can be vectorized. The intention behind this interface is that
 * distance calculations between vectors should leverage SIMD (e.g. AVX, SSE etc.) instruction sets whenever possible.
 * However, the JVMs auto vectorization currently often fails to do so on its own.
 *
 * In the near future with project Panama and JEP 338 (see [1]) becoming reality, vectorization can be done explicitly.
 * In the meanwhile, the JVM can be compelled to vectorize operations by using specific code optimizations, such as manual
 * unrolling of array (see [2]). This interface allows for such specialized implementations.
 *
 * By default, the invocation will fall-back to scalar versions if vectorization is not possible.
 *
 * @see DistanceFunction
 *
 * @author Ralph Gasser
 * @version 1.0
 *
 * [1] https://openjdk.java.net/jeps/338
 * [2] https://richardstartin.github.io/posts/floating-point-manual-unrolling-or-autovectorisation
 */
interface VectorizedDistanceFunction<T> : DistanceFunction<T> {
    /**
     * Calculates the weighted distance between two [VectorValue].
     *
     * @param a First [VectorValue]
     * @param b Second [VectorValue]
     * @param stride The stride to use for vectorization
     *
     * @return Distance between a and b.
     */
    operator fun invoke(a: VectorValue<T>, b: VectorValue<T>, stride: Int): Double = invoke(a, b)

    /**
     * Calculates the weighted distance between two [VectorValue]s
     *
     * @param a First [VectorValue]
     * @param b Second [VectorValue]
     * @param weights The [VectorValue] containing the weights.
     * @param stride The [Shape], i.e., the type of vectorization to use.

     * @return Distance between a and b.
     */
    operator fun invoke(a: VectorValue<T>, b: VectorValue<T>, weights: VectorValue<*>, stride: Int): Double = invoke(a, b, weights)
}