package org.vitrivr.cottontail.core.values

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.core.types.NumericValue
import org.vitrivr.cottontail.core.types.RealValue
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.grpc.CottontailGrpc
import kotlin.math.pow

/**
 * This is an abstraction over a [Double].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
@Serializable
@SerialName("Double")
@JvmInline
value class DoubleValue(override val value: Double): RealValue<Double>, PublicValue{

    companion object {
        /**
         * The minimum value that can be held by a [DoubleValue].
         *
         * Is larger than [Double.MIN_VALUE] because [Double.MIN_VALUE] is reserved to signify null.
         */
        val MIN_VALUE = DoubleValue(Double.MIN_VALUE + Double.MIN_VALUE)

        /** The maximum value that can be held by a [DoubleValue]. */
        val MAX_VALUE = DoubleValue(Float.MAX_VALUE)

        /** The zero [DoubleValue]. */
        val ZERO = DoubleValue(0.0)

        /** The one [DoubleValue]. */
        val ONE = DoubleValue(1.0)

        /** The NaN [DoubleValue]. */
        val NaN = DoubleValue(Double.NaN)

        /** The infinity [DoubleValue]. */
        val INF = DoubleValue(Double.POSITIVE_INFINITY)
    }

    /**
     * Constructor for a [Float]. Applies special conversion to prevent loss in precision
     * in cases that could be prevented.
     *
     * @param number The [Float] that should be converted to a [FloatValue]
     */
    constructor(number: Float) : this(number.toString().toDouble())

    /**
     * Constructor for an arbitrary [Number].
     *
     * @param number The [Number] that should be converted to a [DoubleValue]
     */
    constructor(number: Number) : this(number.toDouble())

    /**
     * Constructor for an arbitrary [NumericValue].
     *
     * @param number The [NumericValue] that should be converted to a [DoubleValue]
     */
    constructor(number: NumericValue<*>) : this(number.value.toDouble())

    override val logicalSize: Int
        get() = 1

    /** The [Types] of this [DoubleValue]. */
    override val type: Types<*>
        get() = Types.Double

    override val real: RealValue<Double>
        get() = this

    override val imaginary: RealValue<Double>
        get() = ZERO

    /**
     * Compares this [DoubleValue] to another [Value]. Returns -1, 0 or 1 of other value is smaller,
     * equal or greater than this value. [DoubleValue] can only be compared to other [NumericValue]s.
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
     * Checks for equality between this [DoubleValue] and the other [Value]. Equality can only be
     * established if the other [Value] is also a [DoubleValue] and holds the same value.
     *
     * @param other [Value] to compare to.
     * @return True if equal, false otherwise.
     */
    override fun isEqual(other: Value): Boolean = (other is DoubleValue) && (other.value == this.value)

    /**
     * Converts this [DoubleValue] to a [CottontailGrpc.Literal] gRCP representation.
     *
     * @return [CottontailGrpc.Literal]
     */
    override fun toGrpc(): CottontailGrpc.Literal
        = CottontailGrpc.Literal.newBuilder().setDoubleData(this.value).build()

    override fun asDouble(): DoubleValue = this
    override fun asFloat(): FloatValue = FloatValue(this.value.toFloat())
    override fun asInt(): IntValue = IntValue(this.value.toInt())
    override fun asLong(): LongValue = LongValue(this.value.toLong())
    override fun asShort(): ShortValue = ShortValue(this.value.toInt().toShort())
    override fun asByte(): ByteValue = ByteValue(this.value.toInt().toByte())
    override fun asComplex32(): Complex32Value = Complex32Value(this.asFloat(), FloatValue(0.0f))
    override fun asComplex64(): Complex64Value = Complex64Value(this.asDouble(), DoubleValue(0.0))

    override fun unaryMinus(): DoubleValue = DoubleValue(-this.value)

    override fun plus(other: NumericValue<*>) = DoubleValue(this.value + other.value.toDouble())
    override fun minus(other: NumericValue<*>) = DoubleValue(this.value - other.value.toDouble())
    override fun times(other: NumericValue<*>) = DoubleValue(this.value * other.value.toDouble())
    override fun div(other: NumericValue<*>) = DoubleValue(this.value / other.value.toDouble())

    override fun abs() = DoubleValue(kotlin.math.abs(this.value))

    override fun pow(x: Double) = DoubleValue(this.value.pow(x))
    override fun pow(x: Int) = DoubleValue(this.value.pow(x))
    override fun sqrt() = DoubleValue(kotlin.math.sqrt(this.value))
    override fun exp() = DoubleValue(kotlin.math.exp(this.value))
    override fun ln() = DoubleValue(kotlin.math.ln(this.value))

    override fun cos() = DoubleValue(kotlin.math.cos(this.value))
    override fun sin() = DoubleValue(kotlin.math.sin(this.value))
    override fun tan() = DoubleValue(kotlin.math.tan(this.value))
    override fun atan() = DoubleValue(kotlin.math.atan(this.value))
}