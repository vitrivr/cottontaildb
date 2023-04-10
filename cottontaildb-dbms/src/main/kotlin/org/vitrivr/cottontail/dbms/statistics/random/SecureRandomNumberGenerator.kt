package org.vitrivr.cottontail.dbms.statistics.random

import kotlinx.serialization.SerialName
import java.security.SecureRandom

/**
 * An implementation of [RandomNumberGenerator] that can be used for sampling.
 * @author Florian Burkhardt
 *  @version 1.0.0
 */
@SerialName("SecureRandomNumberGenerator")
class SecureRandomNumberGenerator : RandomNumberGenerator {
    private val random = SecureRandom()

    override fun nextDouble(): Double {
        return random.nextDouble(0.0, 1.0)
    }
}
