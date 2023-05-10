package org.vitrivr.cottontail.core.values

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.core.types.NumericValue
import org.vitrivr.cottontail.core.types.RealValue
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * This is an abstraction over an [Int].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
@Serializable
@SerialName("Integer")
@JvmInline
value class IntValue(override val value: Int): RealValue<Int>, PublicValue {

    companion object {
        /**
         * The minimum value that can be held by a [IntValue].
         *
         * Is larger than [Int.MIN_VALUE] because [Int.MIN_VALUE] is reserved to signify null.
         */
        val MIN_VALUE = IntValue(Int.MIN_VALUE + 1)

        /** The maximum value that can be held by a [IntValue]. */
        val MAX_VALUE = IntValue(Int.MAX_VALUE)

        /** The zero [IntValue]. */
        val ZERO = IntValue(0)

        /** The zero [IntValue]. */
        val ONE = IntValue(1)
    }

    /**
     * Constructor for an arbitrary [Number].
     *
     * @param number The [Number] that should be converted to a [IntValue]
     */
    constructor(number: Number) : this(number.toInt())

    /**
     * Constructor for an arbitrary [NumericValue].
     *
     * @param number The [NumericValue] that should be converted to a [IntValue]
     */
    constructor(number: NumericValue<*>) : this(number.value.toInt())

    /** The logical size of this [IntValue]*/
    override val logicalSize: Int
        get() = 1

    /** The [Types] of this [IntValue]. */
    override val type: Types<*>
        get() = Types.Int

    override val real: RealValue<Int>
        get() = this

    override val imaginary: RealValue<Int>
        get() = ZERO

    /**
     * Compares this [IntValue] to another [Value]. Returns -1, 0 or 1 of other value is smaller,
     * equal or greater than this value. [IntValue] can only be compared to other [NumericValue]s.
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
     * Checks for equality between this [IntValue] and the other [Value]. Equality can only be
     * established if the other [Value] is also a [IntValue] and holds the same value.
     *
     * @param other [Value] to compare to.
     * @return True if equal, false otherwise.
     */
    override fun isEqual(other: Value): Boolean = (other is IntValue) && (other.value == this.value)

    /**
     * Converts this [IntValue] to a [CottontailGrpc.Literal] gRCP representation.
     *
     * @return [CottontailGrpc.Literal]
     */
    override fun toGrpc(): CottontailGrpc.Literal
        = CottontailGrpc.Literal.newBuilder().setIntData(this.value).build()

    override fun asDouble(): DoubleValue = DoubleValue(this.value.toDouble())
    override fun asFloat(): FloatValue = FloatValue(this.value.toFloat())
    override fun asInt(): IntValue = this
    override fun asLong(): LongValue = LongValue(this.value.toLong())
    override fun asShort(): ShortValue = ShortValue(this.value.toShort())
    override fun asByte(): ByteValue = ByteValue(this.value.toByte())
    override fun asComplex32(): Complex32Value = Complex32Value(this.asFloat(), FloatValue(0.0f))
    override fun asComplex64(): Complex64Value = Complex64Value(this.asDouble(), DoubleValue(0.0))

    override fun unaryMinus(): IntValue = IntValue(-this.value)

    override fun plus(other: NumericValue<*>) = IntValue(this.value + other.value.toInt())
    override fun minus(other: NumericValue<*>) = IntValue(this.value - other.value.toInt())
    override fun times(other: NumericValue<*>) = IntValue(this.value * other.value.toInt())
    override fun div(other: NumericValue<*>) = IntValue(this.value / other.value.toInt())

    override fun abs() = IntValue(kotlin.math.abs(this.value))

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
