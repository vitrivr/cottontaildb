package ch.unibas.dmi.dbis.cottontail.model.values

import ch.unibas.dmi.dbis.cottontail.model.values.complex.Complex

inline class ComplexValue(override val value: Complex) : Value<Complex> {
    override val size: Int
        get() = -1

    override val numeric: Boolean
        get() = true

    override fun compareTo(other: Value<*>): Int = when (other) {
        is BooleanValue -> this.value.compareTo(if (other.value) { Complex(floatArrayOf(1.0f, 0.0f)) } else { Complex(floatArrayOf(0.0f, 0.0f)) })
        is ByteValue -> this.value[0].compareTo(other.value)
        is ShortValue -> this.value[0].compareTo(other.value)
        is IntValue -> this.value[0].compareTo(other.value)
        is LongValue -> this.value[0].compareTo(other.value)
        is DoubleValue -> this.value[0].compareTo(other.value)
        is FloatValue -> this.value[0].compareTo(other.value)
        is ComplexValue -> this.value.compareTo(other.value)
        else -> throw IllegalArgumentException("ComplexValues can only be compared to other numeric values.")
    }
}