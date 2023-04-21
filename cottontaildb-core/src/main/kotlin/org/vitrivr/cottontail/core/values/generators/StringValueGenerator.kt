package org.vitrivr.cottontail.core.values.generators

import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.types.VectorValue
import java.util.random.RandomGenerator

/**
 * A [ValueGenerator] for [StringValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object StringValueGenerator: ValueGenerator<StringValue> {
    /**
     * Generates a random [VectorValue] of up to 4096 characters.
     *
     * @param rnd A [RandomGenerator] to generate the random numbers.
     * @return Random [VectorValue]
     */
    override fun random(rnd: RandomGenerator): StringValue = this.random(rnd.nextInt(4096), rnd)

    /**
     * Generates a random [StringValue].
     *
     * @param size The size of the [StringValue].
     * @param rnd A [RandomGenerator] to generate the random numbers.
     * @return Random [StringValue]
     */
    fun random(size: Int, rnd: RandomGenerator = ValueGenerator.RANDOM): StringValue {
        val builder = StringBuilder()
        while (builder.length < size) {
            val next = rnd.nextInt(128)
            if (next in (48..57) || next in (65..90) || next in (65..90)) {
                builder.appendCodePoint(next)
            }
        }
        return StringValue(builder.toString())
    }

    /**
     * Generates the empty [StringValue].
     *
     * @return Empty [StringValue]
     */
    fun empty() = StringValue.EMPTY
}