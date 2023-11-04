package org.vitrivr.cottontail.core.values.generators

import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.core.values.IntVectorValue
import org.vitrivr.cottontail.core.values.ShortVectorValue
import java.util.*
import java.util.random.RandomGenerator

/**
 * A [VectorValueGenerator] for [ShortVectorValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object ShortVectorValueGenerator: VectorValueGenerator<ShortVectorValue> {
    /**
     * Generates a [ShortVectorValue] of the given size initialized with random numbers.
     *
     * @param size Size of the new [ShortVectorValue]
     * @param rnd A [SplittableRandom] to generate the random numbers.
     * @return [ShortVectorValue]
     */
    override fun random(size: Int, rnd: RandomGenerator) = ShortVectorValue(ShortArray(size) { rnd.nextInt().toShort() })

    /**
     * Generates a [ShortVectorValue] of the given size initialized with ones.
     *
     * @param size Size of the new [ShortVectorValue]
     * @return [ShortVectorValue]
     */
    override fun one(size: Int) = ShortVectorValue(ShortArray(size) { (1).toShort() })

    /**
     * Generates a [IntVectorValue] of the given size initialized with zeros.
     *
     * @param size Size of the new [ShortVectorValue]
     * @return [ShortVectorValue]
     */
    override fun zero(size: Int) = ShortVectorValue(ShortArray(size))

    /**
     * Generates a [ShortVectorValue] given [Array] of [Number]s
     *
     * @param values List of [Number]s to generate the [VectorValue] for.
     * @return [ShortVectorValue]
     */
    override fun with(values: Array<Number>) = ShortVectorValue(values)
}