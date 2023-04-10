package org.vitrivr.cottontail.dbms.statistics.random

import kotlinx.serialization.SerialName
import java.util.Random

/**
 * An implementation of [RandomNumberGenerator] that can be used for sampling.
 * @author Florian Burkhardt
 *  @version 1.0.0
 */
@SerialName("JavaRandomNumberGenerator")
class JavaRandomNumberGenerator : RandomNumberGenerator {
    private val random = Random()

    override fun nextDouble(): Double {
        return random.nextDouble(0.0,1.0)
    }
}