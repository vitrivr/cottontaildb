package org.vitrivr.cottontail.core.values.generators

import org.apache.commons.math3.random.RandomGenerator
import org.vitrivr.cottontail.core.values.DateValue
import kotlin.math.absoluteValue

/**
 * A [ValueGenerator] for [DateValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object DateValueGenerator: ValueGenerator<DateValue> {
    override fun random(rnd: RandomGenerator): DateValue = DateValue(rnd.nextLong().absoluteValue)
    fun now() = DateValue(System.currentTimeMillis())
}