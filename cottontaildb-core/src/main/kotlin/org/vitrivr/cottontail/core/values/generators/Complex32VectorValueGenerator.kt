package org.vitrivr.cottontail.core.values.generators

import org.apache.commons.math3.random.RandomGenerator
import org.vitrivr.cottontail.core.values.Complex32VectorValue
import org.vitrivr.cottontail.core.values.types.VectorValue

/**
 * A [VectorValueGenerator] for [Complex32VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object Complex32VectorValueGenerator: VectorValueGenerator<Complex32VectorValue> {
    /**
     * Generates a [Complex32VectorValue] of the given size initialized with random numbers.
     *
     * @param size Size of the new [Complex32VectorValue]
     * @param rnd A [RandomGenerator] to generate the random numbers.
     * @return Random [Complex32VectorValue]
     */
    override fun random(size: Int, rnd: RandomGenerator) = Complex32VectorValue(FloatArray(size * 2) { rnd.nextFloat() })

    /**
     * Generates a [Complex32VectorValue] of the given size initialized with ones (i.e 1.0f + i0.0f).
     *
     * @param size Size of the new [Complex32VectorValue]
     * @return [Complex32VectorValue] filled with ones.
     */
    override fun one(size: Int) = Complex32VectorValue(FloatArray(size * 2) {
        if (it % 2 == 0) {
            1.0f
        } else {
            0.0f
        }
    })

    /**
     * Generates a [Complex32VectorValue] of the given size initialized with zeros.
     *
     * @param size Size of the new [Complex32VectorValue]
     * @return [Complex32VectorValue] filled with zeros.
     */
    override fun zero(size: Int) = Complex32VectorValue(FloatArray(size * 2) { 0.0f })

    /**
     * Generates a [Complex32VectorValue] given [Array] of [Number]s
     *
     * @param values List of [Number]s to generate the [VectorValue] for.
     * @return [Complex32VectorValue]
     */
    override fun with(values: Array<Number>) = Complex32VectorValue(values)
}