package org.vitrivr.cottontail.core.values.generators

import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.HalfVectorValue
import java.util.random.RandomGenerator

/**
 * A [VectorValueGenerator] for [HalfVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object HalfVectorValueGenerator: VectorValueGenerator<HalfVectorValue> {
    /**
     * Generates a [HalfVectorValue] of the given size initialized with random numbers.
     *
     * @param size Size of the new [HalfVectorValue]
     * @param rnd A [RandomGenerator] to generate the random numbers.
     * @return The generated [HalfVectorValue]
     */
    override fun random(size: Int, rnd: RandomGenerator) = HalfVectorValue(FloatArray(size) { rnd.nextFloat() })

    /**
     * Generates a [HalfVectorValue] of the given size initialized with ones.
     *
     * @param size Size of the new [HalfVectorValue]
     * @return The generated [HalfVectorValue]
     */
    override fun one(size: Int) = HalfVectorValue(FloatArray(size) { 1.0f })

    /**
     * Generates a [FloatVectorValue] of the given size initialized with zeros.
     *
     * @param size Size of the new [HalfVectorValue]
     * @return The generated [HalfVectorValue]
     */
    override fun zero(size: Int) = HalfVectorValue(FloatArray(size))

    /**
     * Generates a [HalfVectorValue] given [Array] of [Number]s
     *
     * @param values List of [Number]s to generate the [VectorValue] for.
     * @return [HalfVectorValue]
     */
    override fun with(values: Array<Number>) = HalfVectorValue(values)
}