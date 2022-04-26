package org.vitrivr.cottontail.core.values.generators

import org.apache.commons.math3.random.RandomGenerator
import org.vitrivr.cottontail.core.values.IntValue

/**
 * A [NumericValueGenerator] for [IntValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object IntValueGenerator: NumericValueGenerator<IntValue> {
    override fun random(rnd: RandomGenerator) = IntValue(rnd.nextInt())
    override fun one() = IntValue.ONE
    override fun zero() = IntValue.ZERO
    override fun of(number: Number) = IntValue(number)
}