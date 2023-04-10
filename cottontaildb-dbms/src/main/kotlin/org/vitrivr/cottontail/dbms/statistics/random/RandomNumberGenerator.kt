package org.vitrivr.cottontail.dbms.statistics.random

import kotlinx.serialization.Polymorphic

/**
 * An interface to provide different Random Number Generators
 * @author Florian Burkhardt
 *  @version 1.0.0
 */

@Polymorphic
interface RandomNumberGenerator {

    /**
     * Returns a random variable between 0 and 1
     */
    fun nextDouble(): Double
}