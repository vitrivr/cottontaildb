package org.vitrivr.cottontail.core.basics


/**
 * An objects that holds [Tuple] values and allows for counting them.
 *
 * @see Tuple
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface Countable {
    /**
     * Returns the number of entries in this [Countable].
     *
     * @return The number of entries in this [Countable].
     */
    fun count(): Long
}