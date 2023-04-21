package org.vitrivr.cottontail.core.values.generators

import org.vitrivr.cottontail.core.values.LongValue
import java.util.random.RandomGenerator

/**
 * A [NumericValueGenerator] for [LongValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object LongValueGenerator: NumericValueGenerator<LongValue> {
    override fun random(rnd: RandomGenerator): LongValue = LongValue(rnd.nextLong())
    override fun one(): LongValue = LongValue.ONE
    override fun zero(): LongValue = LongValue.ZERO
    override fun of(number: Number): LongValue = LongValue(number)
}