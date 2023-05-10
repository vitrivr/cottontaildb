package org.vitrivr.cottontail.core.values

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.core.types.NumericValue
import org.vitrivr.cottontail.core.types.RealValue
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * This is an abstraction over a [Long].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
@Serializable
@SerialName("Long")
@JvmInline
value class LongValue(override val value: Long): RealValue<Long>, PublicValue {

    companion object {
        /**
         * The minimum value that can be held by a [LongValue].
         *
         * Is larger than [Long.MIN_VALUE] because [Long.MIN_VALUE] is reserved to signify null.
         */
        val MIN_VALUE = LongValue(Long.MIN_VALUE + 1L)

        /** The maximum value that can be held by a [LongValue]. */
        val MAX_VALUE = LongValue(Long.MAX_VALUE)

        /** The zero [LongValue]. */
        val ZERO = LongValue(0L)

        /** The one [LongValue]. */
        val ONE = LongValue(1L)
    }

    /**
     * Constructor for an arbitrary [Number].
     *
     * @param number The [Number] that should be converted to a [LongValue]
     */
    constructor(number: Number) : this(number.toLong())

    /**
     * Constructor for an arbitrary [NumericValue].
     *
     * @param number The [NumericValue] that should be converted to a [LongValue]
     */
    constructor(number: NumericValue<*>) : this(number.value.toLong())

    /** The logical size of this [LongValue]. */
    override val logicalSize: Int
        get() = 1

    /** The [Types] of this [LongValue]. */
    override val type: Types<*>
        get() = Types.Long

    override val real: RealValue<Long>
        get() = this

    override val imaginary: RealValue<Long>
        get() = ZERO

    /**
     * Compares this [LongValue] to another [Value]. Returns -1, 0 or 1 of other value is smaller,
     * equal or greater than this value. [LongValue] can only be compared to other [NumericValue]s.
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
    override fun isEqual(other: Value): Boolean = (other is LongValue) && (other.value == this.value)

    /**
     * Converts this [LongValue] to a [CottontailGrpc.Literal] gRCP representation.
     *
     * @return [CottontailGrpc.Literal]
     */
    override fun toGrpc(): CottontailGrpc.Literal
        = CottontailGrpc.Literal.newBuilder().setLongData(this.value).build()


    override fun asDouble(): DoubleValue = DoubleValue(this.value.toDouble())
    override fun asFloat(): FloatValue = FloatValue(this.value.toFloat())
    override fun asInt(): IntValue = IntValue(this.value.toInt())
    override fun asLong(): LongValue = this
    override fun asShort(): ShortValue = ShortValue(this.value.toShort())
    override fun asByte(): ByteValue = ByteValue(this.value.toByte())
    override fun asComplex32(): Complex32Value = Complex32Value(this.asFloat(), FloatValue(0.0f))
    override fun asComplex64(): Complex64Value = Complex64Value(this.asDouble(), DoubleValue(0.0))

    override fun unaryMinus(): LongValue = LongValue(-this.value)

    override fun plus(other: NumericValue<*>) = LongValue(this.value + other.value.toLong())
    override fun minus(other: NumericValue<*>) = LongValue(this.value - other.value.toLong())
    override fun times(other: NumericValue<*>) = LongValue(this.value * other.value.toLong())
    override fun div(other: NumericValue<*>) = LongValue(this.value / other.value.toLong())

    override fun abs() = LongValue(kotlin.math.abs(this.value))

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