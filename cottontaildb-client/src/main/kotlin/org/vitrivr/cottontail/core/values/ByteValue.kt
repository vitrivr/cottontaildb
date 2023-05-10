package org.vitrivr.cottontail.core.values

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.core.types.NumericValue
import org.vitrivr.cottontail.core.types.RealValue
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * This is an abstraction over a [Byte].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
@Serializable
@SerialName("Byte")
@JvmInline
value class ByteValue(override val value: Byte): RealValue<Byte>, PublicValue {

    companion object {
        /**
         * The minimum value that can be held by a [ByteValue].
         *
         * Is larger than [Byte.MIN_VALUE] because [Byte.MIN_VALUE] is reserved to signify null.
         */
        val MIN_VALUE = ByteValue(Byte.MIN_VALUE + 1)

        /** The maximum value that can be held by a [ByteValue]. */
        val MAX_VALUE = ByteValue(Byte.MAX_VALUE)

        /** The zero [ByteValue]. */
        val ZERO = ByteValue(0.toByte())

        /** The one [ByteValue]. */
        val ONE = ByteValue(1.toByte())
    }

    /**
     * Constructor for an arbitrary [Number].
     *
     * @param number The [Number] that should be converted to a [ShortValue]
     */
    constructor(number: Number) : this(number.toByte())

    /** The logical size of this [ByteValue]. */
    override val logicalSize: Int
        get() = 1

    /** The [Types] of this [ByteValue]. */
    override val type: Types<*>
        get() = Types.Byte

    override val real: RealValue<Byte>
        get() = this

    override val imaginary: RealValue<Byte>
        get() = ZERO

    /**
     * Compares this [ByteValue] to another [Value]. Returns -1, 0 or 1 of other value is smaller,
     * equal or greater than this value. [ByteValue] can only be compared to other [NumericValue]s.
     *
     * @param other Value to compare to.
     * @return -1, 0 or 1 of other value is smaller, equal or greater than this value
     */
    override fun compareTo(other: Value): Int = when (other) {
        is ByteValue -> this.value.compareTo(other.value)
        is ShortValue -> this.value.compareTo(other.value)
        is IntValue -> this.value.compareTo(other.value)
        is LongValue -> this.value.compareTo(other.value)
        is DoubleValue -> this.value.compareTo(other.value)
        is FloatValue -> this.value.compareTo(other.value)
        is Complex32Value -> this.value.compareTo(other.data[0])
        is Complex64Value -> this.value.compareTo(other.data[0])
        else -> throw IllegalArgumentException("LongValues can only be compared to other numeric values.")
    }

    /**
     * Checks for equality between this [ByteValue] and the other [Value]. Equality can only be
     * established if the other [Value] is also a [ByteValue] and holds the same value.
     *
     * @param other [Value] to compare to.
     * @return True if equal, false otherwise.
     */
    override fun isEqual(other: Value): Boolean = (other is ByteValue) && (other.value == this.value)

    /**
     * Converts this [ByteValue] to a [CottontailGrpc.Literal] gRCP representation.
     *
     * @return [CottontailGrpc.Literal]
     */
    override fun toGrpc(): CottontailGrpc.Literal
        = CottontailGrpc.Literal.newBuilder().setIntData(this.value.toInt()).build()

    override fun asDouble(): DoubleValue = DoubleValue(this.value.toDouble())
    override fun asFloat(): FloatValue = FloatValue(this.value.toFloat())
    override fun asInt(): IntValue = IntValue(this.value.toInt())
    override fun asLong(): LongValue = LongValue(this.value.toLong())
    override fun asShort(): ShortValue = ShortValue(this.value.toShort())
    override fun asByte(): ByteValue = this
    override fun asComplex32(): Complex32Value = Complex32Value(this.asFloat(), FloatValue(0.0f))
    override fun asComplex64(): Complex64Value = Complex64Value(this.asDouble(), DoubleValue(0.0))

    override fun unaryMinus(): ByteValue = ByteValue((-this.value).toByte())

    override fun plus(other: NumericValue<*>) = ByteValue((this.value + other.value.toByte()).toByte())
    override fun minus(other: NumericValue<*>) = ByteValue((this.value - other.value.toByte()).toByte())
    override fun times(other: NumericValue<*>) = ByteValue((this.value * other.value.toByte()).toByte())
    override fun div(other: NumericValue<*>) = ByteValue((this.value / other.value.toByte()).toByte())

    override fun abs() = ByteValue(kotlin.math.abs(this.value.toInt()).toByte())

    override fun pow(x: Int) = this.asDouble().pow(x)
    override fun pow(x: Double) = this.asDouble().pow(x)
    override fun sqrt() = this.asDouble().sqrt()
    override fun exp() = this.asDouble().exp()
    override fun ln() = this.asDouble().ln()

    override fun cos() = this.asDouble().cos()
    override fun sin() = this.asDouble().sin()
    override fun tan() = this.asDouble().tan()
    override fun atan() = this.asDouble().atan()
}