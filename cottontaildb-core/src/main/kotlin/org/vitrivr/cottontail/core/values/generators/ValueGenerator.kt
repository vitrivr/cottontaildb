package org.vitrivr.cottontail.core.values.generators

import org.vitrivr.cottontail.core.types.Value
import java.util.*
import java.util.random.RandomGenerator

/**
 * A class that can be used to generate a certain type of [ValueGenerator].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
interface ValueGenerator<T: Value> {
    companion object {
        /** Internal [RandomGenerator] instance used for generation of random [Value]s. */
        @JvmStatic
        val RANDOM: RandomGenerator = SplittableRandom()
    }

    /**
     * Generates a new, random [Value] of type [T].
     *
     * @param rnd The [RandomGenerator] to use.
     * @return [T]
     */
    fun random(rnd: RandomGenerator = RANDOM): T
}