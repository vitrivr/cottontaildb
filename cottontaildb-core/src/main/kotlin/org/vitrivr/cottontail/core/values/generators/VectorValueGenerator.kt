package org.vitrivr.cottontail.core.values.generators

import org.vitrivr.cottontail.core.values.types.VectorValue
import java.util.random.RandomGenerator

/**
 * A [ValueGenerator] for [VectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface VectorValueGenerator<T: VectorValue<*>>: ValueGenerator<T> {
    /**
     * Generates a random [VectorValue] of up to 4096 components.
     *
     * @param rnd A [RandomGenerator] to generate the random numbers.
     * @return Random [VectorValue]
     */
    override fun random(rnd: RandomGenerator) = this.random(rnd.nextInt(4096), rnd)

    /**
     * Generates a [VectorValue] of the given size initialized with random numbers.
     *
     * @param size Size of the new [VectorValue]
     * @param rnd A [RandomGenerator] to generate the random numbers.
     * @return Random [VectorValue] of size [size]
     */
    fun random(size: Int, rnd: RandomGenerator = ValueGenerator.RANDOM): T

    /**
     * Generates a [VectorValue] of the given size initialized with ones.
     *
     * @param size Size of the new [VectorValue]
     * @return [VectorValue] of size [size] containing ones.
     */
    fun one(size: Int): T

    /**
     * Generates a [VectorValue] of the given size initialized with zeros.
     *
     * @param size Size of the new [VectorValue]
     * @return [VectorValue] of size [size] containing zeros.
     */
    fun zero(size: Int): T

    /**
     * Generates a [VectorValue] given [Array] of [Number]s
     *
     * @param values List of [Number]s to generate the [VectorValue] for.
     */
    fun with(values: Array<Number>): T
}