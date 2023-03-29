package org.vitrivr.cottontail.core.values.generators

import org.vitrivr.cottontail.core.values.BooleanValue
import java.util.random.RandomGenerator

/**
 * A [ValueGenerator] for [BooleanValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object BooleanValueGenerator: ValueGenerator<BooleanValue> {
    override fun random(rnd: RandomGenerator) = BooleanValue(rnd.nextBoolean())
    fun ofTrue() = BooleanValue.TRUE
    fun ofFalse() = BooleanValue.FALSE
}