package org.vitrivr.cottontail.core.values.generators

import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.core.values.Complex64VectorValue
import java.util.random.RandomGenerator

/**
 * A [VectorValueGenerator] for [Complex64VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object Complex64VectorValueGenerator: VectorValueGenerator<Complex64VectorValue> {
    /**
     * Generates a [Complex64VectorValue] of the given size initialized with random numbers.
     *
     * @param size Size of the new [Complex64VectorValue]
     * @param rnd A [RandomGenerator] to generate the random numbers.
     * @return Random [Complex64VectorValue]
     */
    override fun random(size: Int, rnd: RandomGenerator) = Complex64VectorValue(DoubleArray(2 * size) { rnd.nextDouble() })

    /**
     * Generates a [Complex64VectorValue] of the given size initialized with ones (i.e 1.0f + i0.0f).
     *
     * @param size Size of the new [Complex64VectorValue]
     * @return [Complex64VectorValue] filled with ones.
     */
    override fun one(size: Int) = Complex64VectorValue(DoubleArray(size * 2) {
        if (it % 2 == 0) {
            1.0
        } else {
            0.0
        }
    })

    /**
     * Generates a [Complex64VectorValue] of the given size initialized with zeros.
     *
     * @param size Size of the new [Complex64VectorValue]
     * @return [Complex64VectorValue] filled with zeros.
     */
    override fun zero(size: Int) = Complex64VectorValue(DoubleArray(size * 2) { 0.0 })

    /**
     * Generates a [Complex64VectorValue] given [Array] of [Number]s
     *
     * @param values List of [Number]s to generate the [VectorValue] for.
     * @return [Complex64VectorValue]
     */
    override fun with(values: Array<Number>) = Complex64VectorValue(values)
}