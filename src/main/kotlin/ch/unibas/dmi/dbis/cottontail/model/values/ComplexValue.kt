package ch.unibas.dmi.dbis.cottontail.model.values

import ch.unibas.dmi.dbis.cottontail.model.values.complex.Complex

inline class ComplexValue(override val value: Complex) : Value<Complex> {
    override val size: Int
        get() = -1

    override val numeric: Boolean
        get() = true

    override fun compareTo(other: Value<*>): Int = when (other) {
        is ComplexValue -> this.value.compareTo(other.value)
        else -> throw IllegalArgumentException("ComplexValues can only be compared to other ComplexValues.")
    }
}