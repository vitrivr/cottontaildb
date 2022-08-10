package org.vitrivr.cottontail.core.values.generators

import org.vitrivr.cottontail.core.values.DateValue
import java.util.random.RandomGenerator
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