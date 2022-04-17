package org.vitrivr.cottontail.core.values.generators

import org.apache.commons.math3.random.RandomGenerator
import org.vitrivr.cottontail.core.values.Complex64Value

/**
 * A [NumericValueGenerator] for [Complex64Value]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object Complex64ValueGenerator: NumericValueGenerator<Complex64Value> {
    override fun random(rnd: RandomGenerator) = Complex64Value(rnd.nextDouble(), rnd.nextDouble())
    override fun one() = Complex64Value.ONE
    override fun zero() = Complex64Value.ZERO
    override fun of(number: Number) = Complex64Value(number)
}