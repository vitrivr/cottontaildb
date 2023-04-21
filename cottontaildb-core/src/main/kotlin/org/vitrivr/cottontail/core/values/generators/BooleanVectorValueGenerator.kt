package org.vitrivr.cottontail.core.values.generators

import org.vitrivr.cottontail.core.values.BooleanVectorValue
import org.vitrivr.cottontail.core.values.IntVectorValue
import org.vitrivr.cottontail.core.values.types.VectorValue
import java.util.random.RandomGenerator

/**
 * A [VectorValueGenerator] for [BooleanVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object BooleanVectorValueGenerator: VectorValueGenerator<BooleanVectorValue> {
    /**
     * Generates a [IntVectorValue] of the given size initialized with random numbers.
     *
     * @param size Size of the new [IntVectorValue]
     * @param rnd A [RandomGenerator] to generate the random numbers.
     * @return Random [BooleanVectorValue] of size [size]
     */
    override fun random(size: Int, rnd: RandomGenerator) = BooleanVectorValue(BooleanArray(size) { rnd.nextBoolean() })

    /**
     * Generates a [IntVectorValue] of the given size initialized with ones.
     *
     * @param size Size of the new [IntVectorValue]
     */
    override fun one(size: Int) = BooleanVectorValue(BooleanArray(size) { true })

    /**
     * Generates a [IntVectorValue] of the given size initialized with zeros.
     *
     * @param size Size of the new [IntVectorValue]
     */
    override fun zero(size: Int) = BooleanVectorValue(BooleanArray(size))

    /**
     * Generates a [BooleanVectorValue] given [Array] of [Number]s
     *
     * @param values List of [Number]s to generate the [VectorValue] for.
     * @return [BooleanVectorValue]
     */
    override fun with(values: Array<Number>) = BooleanVectorValue(values)
}