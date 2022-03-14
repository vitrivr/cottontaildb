package org.vitrivr.cottontail.core.values.generators

import org.apache.commons.math3.random.RandomGenerator
import org.vitrivr.cottontail.core.values.DoubleValue

/**
 * A [NumericValueGenerator] for [DoubleValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object DoubleValueGenerator: NumericValueGenerator<DoubleValue> {
    override fun random(rnd: RandomGenerator) = DoubleValue(rnd.nextDouble())
    override fun one() = DoubleValue.ONE
    override fun zero() = DoubleValue.ZERO
    override fun of(number: Number) = DoubleValue(number)
}