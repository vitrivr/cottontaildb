package org.vitrivr.cottontail.dbms.statistics.random

import kotlinx.serialization.SerialName
import kotlin.random.Random

/**
 * An implementation of [RandomNumberGenerator] that can be used for sampling.
 * @author Florian Burkhardt
 *  @version 1.0.0
 */
@SerialName("KotlinRandomNumberGenerator")
class KotlinRandomNumberGenerator : RandomNumberGenerator {
    private val random = Random.Default

    override fun nextDouble(): Double {
        return random.nextDouble(0.0, 1.0)
    }
}
