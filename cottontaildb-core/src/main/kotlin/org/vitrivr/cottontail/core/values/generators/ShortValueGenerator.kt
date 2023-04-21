package org.vitrivr.cottontail.core.values.generators

import org.vitrivr.cottontail.core.values.ByteValue
import org.vitrivr.cottontail.core.values.ShortValue
import java.util.random.RandomGenerator

/**
 * A [NumericValueGenerator] for [ByteValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object ShortValueGenerator: NumericValueGenerator<ShortValue> {
    override fun random(rnd: RandomGenerator) = ShortValue(rnd.nextInt(Short.MAX_VALUE.toInt()).toShort())
    override fun one() = ShortValue.ONE
    override fun zero() = ShortValue.ZERO
    override fun of(number: Number) = ShortValue(number)
}