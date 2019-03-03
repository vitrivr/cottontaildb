package ch.unibas.dmi.dbis.cottontail.model.basics

/**
 *
 */
interface Countable {
    /**
     * Returns the number of entries in this [Countable].
     *
     * @return The number of entries in this [Countable].
     */
    fun count(): Long
}