package org.vitrivr.cottontail.core.values.generators

import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.types.VectorValue
import java.util.random.RandomGenerator

/**
 * A [VectorValueGenerator] for [DoubleVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object FloatVectorValueGenerator: VectorValueGenerator<FloatVectorValue> {
    /**
     * Generates a [FloatVectorValue] of the given size initialized with random numbers.
     *
     * @param size Size of the new [FloatVectorValue]
     * @param rnd A [RandomGenerator] to generate the random numbers.
     * @return The generated [FloatVectorValue]
     */
    override fun random(size: Int, rnd: RandomGenerator) = FloatVectorValue(FloatArray(size) { rnd.nextFloat() })

    /**
     * Generates a [FloatVectorValue] of the given size initialized with ones.
     *
     * @param size Size of the new [FloatVectorValue]
     * @return The generated [FloatVectorValue]
     */
    override fun one(size: Int) = FloatVectorValue(FloatArray(size) { 1.0f })

    /**
     * Generates a [FloatVectorValue] of the given size initialized with zeros.
     *
     * @param size Size of the new [FloatVectorValue]
     * @return The generated [FloatVectorValue]
     */
    override fun zero(size: Int) = FloatVectorValue(FloatArray(size))

    /**
     * Generates a [FloatVectorValue] given [Array] of [Number]s
     *
     * @param values List of [Number]s to generate the [VectorValue] for.
     * @return [FloatVectorValue]
     */
    override fun with(values: Array<Number>) = FloatVectorValue(values)
}