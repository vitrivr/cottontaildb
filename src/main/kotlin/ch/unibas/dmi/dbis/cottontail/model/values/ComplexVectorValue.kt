package ch.unibas.dmi.dbis.cottontail.model.values

import ch.unibas.dmi.dbis.cottontail.model.values.complex.ComplexArray

/**
 * This is an abstraction over a [ComplexArray] and it represents a vector of [Complex]s.
 *
 * @author Manuel Huerbin
 * @version 1.0
 */
inline class ComplexVectorValue(override val value: ComplexArray) : Value<ComplexArray> {

    override val size: Int
        get() = this.value.size

    override val numeric: Boolean
        get() = false

    override fun compareTo(other: Value<*>): Int {
        throw IllegalArgumentException("ComplexVectorValues can can only be compared for equality.")
    }

    // TODO
}