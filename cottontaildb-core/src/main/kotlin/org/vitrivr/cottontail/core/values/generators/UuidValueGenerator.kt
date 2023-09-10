package org.vitrivr.cottontail.core.values.generators

import org.vitrivr.cottontail.core.values.UuidValue
import java.util.*
import java.util.random.RandomGenerator

/**
 * A [ValueGenerator] for [UuidValue]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object UuidValueGenerator: ValueGenerator<UuidValue> {
    /** The NIL [UuidValue]. */
    val NIL = UuidValue("00000000-0000-0000-0000-000000000000")

    /**
     * Generates and returns a random [UuidValue].
     *
     * @param rnd [RandomGenerator]. This is being ignored!
     * @return [UuidValue]
     */
    override fun random(rnd: RandomGenerator): UuidValue = UuidValue(UUID.randomUUID())
}