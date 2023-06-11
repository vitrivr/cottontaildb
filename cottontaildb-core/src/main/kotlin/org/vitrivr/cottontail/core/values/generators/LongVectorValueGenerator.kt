package org.vitrivr.cottontail.core.values.generators

import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.core.values.IntVectorValue
import org.vitrivr.cottontail.core.values.LongVectorValue
import java.util.*
import java.util.random.RandomGenerator

/**
 * A [VectorValueGenerator] for [LongVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object LongVectorValueGenerator: VectorValueGenerator<LongVectorValue> {
    /**
     * Generates a [LongVectorValue] of the given size initialized with random numbers.
     *
     * @param size Size of the new [LongVectorValue]
     * @param rnd A [SplittableRandom] to generate the random numbers.
     * @return [LongVectorValue]
     */
    override fun random(size: Int, rnd: RandomGenerator) = LongVectorValue(LongArray(size) { rnd.nextLong() })

    /**
     * Generates a [LongVectorValue] of the given size initialized with ones.
     *
     * @param size Size of the new [LongVectorValue]
     * @return [LongVectorValue]
     */
    override fun one(size: Int) = LongVectorValue(LongArray(size) { 1L })

    /**
     * Generates a [IntVectorValue] of the given size initialized with zeros.
     *
     * @param size Size of the new [LongVectorValue]
     * @return [LongVectorValue]
     */
    override fun zero(size: Int) = LongVectorValue(LongArray(size))

    /**
     * Generates a [LongVectorValue] given [Array] of [Number]s
     *
     * @param values List of [Number]s to generate the [VectorValue] for.
     * @return [LongVectorValue]
     */
    override fun with(values: Array<Number>) = LongVectorValue(values)
}