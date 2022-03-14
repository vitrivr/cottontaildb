package org.vitrivr.cottontail.core.values.generators

import org.apache.commons.math3.random.RandomGenerator
import org.vitrivr.cottontail.core.values.Complex32Value

/**
 * A [NumericValueGenerator] for [Complex32Value]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object Complex32ValueGenerator: NumericValueGenerator<Complex32Value> {
    override fun random(rnd: RandomGenerator) = Complex32Value(rnd.nextFloat(), rnd.nextFloat())
    override fun one() = Complex32Value.ONE
    override fun zero() = Complex32Value.ZERO
    override fun of(number: Number) = Complex32Value(number)
}