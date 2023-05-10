package org.vitrivr.cottontail.core.values

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.core.types.NumericValue
import org.vitrivr.cottontail.core.types.RealValue
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * This is an abstraction over a [Short].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
@Serializable
@SerialName("Short")
@JvmInline
value class ShortValue(override val value: Short): RealValue<Short>, PublicValue {
    companion object {
        /**
         * The minimum value that can be held by a [ShortValue].
         *
         * Is larger than [Short.MIN_VALUE] because [Short.MIN_VALUE] is reserved to signify null.
         */
        val MIN_VALUE = ShortValue(Short.MIN_VALUE + 1.toShort())

        /** The maximum value that can be held by a [ShortValue]. */
        val MAX_VALUE = ShortValue(Short.MAX_VALUE)

        /** The zero [ShortValue]. */
        val ZERO = ShortValue(0.toShort())

        /** The one [ShortValue]. */
        val ONE = ShortValue(1.toShort())
    }

    /**
     * Constructor for an arbitrary [Number].
     *
     * @param number The [Number] that should be converted to a [ShortValue]
     */
    constructor(number: Number) : this(number.toShort())

    /**
     * Constructor for an arbitrary [NumericValue].
     *
     * @param number The [NumericValue] that should be converted to a [ShortValue]
     */
    constructor(number: NumericValue<*>) : this(number.value.toShort())

    /** The logical size of this [ShortValue]. */
    override val logicalSize: Int
        get() = 1

    /** The [Types] of this [ShortValue]. */
    override val type: Types<*>
        get() = Types.Short

    override val real: RealValue<Short>
        get() = this

    override val imaginary: RealValue<Short>
        get() = ZERO

    /**
     * Compares this [ShortValue] to another [Value]. Returns -1, 0 or 1 of other value is smaller,
     * equal or greater than this value. [ShortValue] can only be compared to other [NumericValue]s.
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
     * Checks for equality between this [LongValue] and the other [Value]. Equality can only be
     * established if the other [Value] is also a [LongValue] and holds the same value.
     *
     * @param other [Value] to compare to.
     * @return True if equal, false otherwise.
     */
    override fun isEqual(other: Value): Boolean = (other is ShortValue) && (other.value == this.value)

    /**
     * Converts this [ShortValue] to a [CottontailGrpc.Literal] gRCP representation.
     *
     * @return [CottontailGrpc.Literal]
     */
    override fun toGrpc(): CottontailGrpc.Literal
        = CottontailGrpc.Literal.newBuilder().setIntData(this.value.toInt()).build()

    override fun asDouble(): DoubleValue = DoubleValue(this.value.toDouble())
    override fun asFloat(): FloatValue = FloatValue(this.value.toFloat())
    override fun asInt(): IntValue = IntValue(this.value.toInt())
    override fun asLong(): LongValue = LongValue(this.value.toLong())
    override fun asShort(): ShortValue = this
    override fun asByte(): ByteValue = ByteValue(this.value.toByte())
    override fun asComplex32(): Complex32Value = Complex32Value(this.asFloat(), FloatValue(0.0f))
    override fun asComplex64(): Complex64Value = Complex64Value(this.asDouble(), DoubleValue(0.0))

    override fun unaryMinus(): ShortValue = ShortValue((-this.value).toShort())

    override fun plus(other: NumericValue<*>) = ShortValue((this.value + other.value.toShort()).toShort())
    override fun minus(other: NumericValue<*>) = ShortValue((this.value - other.value.toShort()).toShort())
    override fun times(other: NumericValue<*>) = ShortValue((this.value * other.value.toShort()).toShort())
    override fun div(other: NumericValue<*>) = ShortValue((this.value / other.value.toShort()).toShort())

    override fun abs() = ShortValue(kotlin.math.abs(this.value.toInt()))

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