package org.vitrivr.cottontail.core.values.generators

import org.apache.commons.math3.random.JDKRandomGenerator
import org.apache.commons.math3.random.RandomGenerator
import org.vitrivr.cottontail.core.values.types.Value

/**
 * A class that can be used to generate a certain type of [ValueGenerator].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface ValueGenerator<T: Value> {
    companion object {
        /** Internal [JDKRandomGenerator] instance used for generation of random [Value]s. */
        @JvmStatic
        val RANDOM = JDKRandomGenerator()
    }

    /**
     * Generates a new, random [Value] of type [T].
     *
     * @param rnd The [RandomGenerator] to use.
     * @return [T]
     */
    fun random(rnd: RandomGenerator = JDKRandomGenerator()): T
}