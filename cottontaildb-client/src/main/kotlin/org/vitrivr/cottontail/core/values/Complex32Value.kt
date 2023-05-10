package org.vitrivr.cottontail.core.values

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.core.types.ComplexValue
import org.vitrivr.cottontail.core.types.NumericValue
import org.vitrivr.cottontail.core.types.RealValue
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.types.*
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * Represents a complex number backed by single-precision (32bit) [Float]s
 *
 * @version 2.0.0
 * @author Ralph Gasser
 */
@Serializable
@SerialName("Complex32")
@JvmInline
value class Complex32Value(val data: FloatArray): ComplexValue<Float>, PublicValue {

    companion object {
        val I = Complex32Value(floatArrayOf(0.0f, 1.0f))
        val ZERO = Complex32Value(floatArrayOf(0.0f, 0.0f))
        val ONE = Complex32Value(floatArrayOf(1.0f, 0.0f))
        val NaN = Complex32Value(floatArrayOf(Float.NaN, Float.NaN))
        val INF = Complex32Value(floatArrayOf(Float.POSITIVE_INFINITY, Float.POSITIVE_INFINITY))
    }

    /**
     * Constructor for one [Float]
     *
     * @param real The real part of the [Complex32Value].
     */
    constructor(real: Float) : this(floatArrayOf(real, 0.0f))

    /**
     * Constructor for an arbitrary [Number].
     *
     * @param number The [Number] that should be converted to a [DoubleValue]
     */
    constructor(number: Number) : this(floatArrayOf(number.toFloat(), 0.0f))

    /**
     * Constructor for an arbitrary [NumericValue].
     *
     * @param number The [NumericValue] that should be converted to a [DoubleValue]
     */
    constructor(number: NumericValue<*>): this(when (number) {
        is ComplexValue<*> -> floatArrayOf(number.real.value.toFloat(), number.imaginary.value.toFloat())
        else -> floatArrayOf(number.value.toFloat(), 0.0f)
    })

    /**
     * Constructor for two [RealValue]s
     *
     * @param real The real part of the [Complex32Value].
     * @param imaginary The imaginary part of the [Complex32Value].
     */
    constructor(real: RealValue<*>, imaginary: RealValue<*>) : this(real.value, imaginary.value)

    /**
     * Constructor for two [Float]s
     *
     * @param real The real part of the [Complex32Value].
     * @param imaginary The imaginary part of the [Complex32Value].
     */
    constructor(real: Float, imaginary: Float) : this(floatArrayOf(real, imaginary))

    /**
     * Constructor for two [Number]s
     *
     * @param real The real part of the [Complex32Value].
     * @param imaginary The imaginary part of the [Complex32Value].
     */
    constructor(real: Number, imaginary: Number) : this(floatArrayOf(real.toFloat(), imaginary.toFloat()))

    override val value: Float
        get() = this.data[0]

    override val real: FloatValue
        get() = FloatValue(this.data[0])

    override val imaginary: FloatValue
        get() = FloatValue(this.data[1])

    /** The logical size of this [Complex32Value]. */
    override val logicalSize: Int
        get() = 1

    /** The [Types] of this [Complex32Value]. */
    override val type: Types<*>
        get() = Types.Complex32

    /**
     * [Complex32Value]s cannot be compared to other [Value]s.
     */
    override fun compareTo(other: Value): Int  {
        throw IllegalArgumentException("Complex32Values can can only be compared for equality.")
    }

    /**
     * Checks for equality between this [Complex32Value] and the other [Value]. Equality can only be
     * established if the other [Value] is also a [Complex32Value] and holds the same value.
     *
     * @param other [Value] to compare to.
     * @return True if equal, false otherwise.
     */
    override fun isEqual(other: Value): Boolean = (other is Complex32Value) && (this.data.contentEquals(other.data))

    /**
     * Converts this [Complex32Value] to a [CottontailGrpc.Literal] gRCP representation.
     *
     * @return [CottontailGrpc.Literal]
     */
    override fun toGrpc(): CottontailGrpc.Literal
        = CottontailGrpc.Literal.newBuilder().setComplex32Data(CottontailGrpc.Complex32.newBuilder().setReal(this.data[0]).setImaginary(this.data[1])).build()

    override fun asComplex32(): Complex32Value = this
    override fun asComplex64(): Complex64Value = Complex64Value(this.data[0], data[1])

    /**
     * Calculates and returns the inverse of this [Complex32Value].
     *
     * @return The inverse [Complex32Value].
     */
    override fun inverse(): Complex32Value = Complex32Value((this.data[0] / (this.data[0] * this.data[0] + this.data[1] * this.data[1])), -(this.data[1] / (this.data[0] * this.data[0] + this.data[1] * this.data[1])))

    /**
     * Returns the complex conjugate of this [Complex32Value]
     *
     * @return The conjugate [Complex32Value].
     */
    override fun conjugate(): Complex32Value = Complex32Value(this.data[0], -this.data[1])

    override fun unaryMinus() = Complex32Value(-this.data[0], -this.data[1])

    override fun plus(other: NumericValue<*>) = when (other) {
        is Complex32Value -> Complex32Value(this.data[0] + other.data[0], this.data[1] + other.data[1])
        is Complex64Value -> Complex32Value(this.data[0] + other.data[0], this.data[1] + other.data[1])
        else -> Complex32Value(this.data[0] + other.real.asDouble().value, this.data[1])
    }

    override fun minus(other: NumericValue<*>) = when (other) {
        is Complex32Value -> Complex32Value(this.data[0] - other.data[0], this.data[1] - other.data[1])
        is Complex64Value -> Complex32Value(this.data[0] - other.data[0], this.data[1] - other.data[1])
        else -> Complex32Value(this.data[0] - other.real.asDouble().value, this.data[1])
    }

    override fun times(other: NumericValue<*>) = when (other) {
        is Complex32Value -> Complex32Value(this.data[0] * other.data[0] - this.data[1] * other.data[1], this.data[0] * other.data[1] + this.data[1] * other.data[0])
        is Complex64Value -> Complex32Value(this.data[0] * other.data[0] - this.data[1] * other.data[1], this.data[0] * other.data[1] + this.data[1] * other.data[0])
        else -> Complex32Value(this.data[0] * other.real.asDouble().value, this.data[1] * other.real.asDouble().value)
    }

    override fun div(other: NumericValue<*>): Complex32Value = when (other) {
        is Complex32Value -> {
            val c = other.data[0]
            val d = other.data[1]
            if (kotlin.math.abs(c) < kotlin.math.abs(d)) {
                val q = c / d
                val denominator = c * q + d
                Complex32Value((this.data[0] * q + this.data[1]) / denominator, (this.data[1] * q - this.data[0]) / denominator)
            } else {
                val q = d / c
                val denominator = d * q + c
                Complex32Value((this.data[1] * q + this.data[0]) / denominator, (this.data[1] - this.data[0] * q) / denominator)
            }
        }
        is Complex64Value -> {
            if (kotlin.math.abs(other.data[0]) < kotlin.math.abs(other.data[1])) {
                val q = other.data[0] / other.data[1]
                val denominator = other.data[0] * q + other.data[1]
                Complex32Value((this.data[0] * q + this.data[1]) / denominator, (this.data[1] * q - this.data[0]) / denominator)
            } else {
                val q = other.data[1] / other.data[0]
                val denominator = other.data[1] * q + other.data[0]
                Complex32Value((this.data[1] * q + this.data[0]) / denominator, (this.data[1] - this.data[0] * q) / denominator)
            }
        }
        else -> {
            val c = other.asFloat().value
            Complex32Value(this.data[0] / c, this.data[1] / c)
        }
    }

    override fun abs() = FloatValue(kotlin.math.sqrt(this.data[0] * this.data[0] + this.data[1] * this.data[1]))

    override fun pow(x: Double): Complex64Value {
        val real = x * kotlin.math.ln(this.abs().value)
        val imaginary = x * kotlin.math.atan2(this.data[1], this.data[0])
        val exp = kotlin.math.exp(real)
        return Complex64Value(exp * kotlin.math.cos(imaginary), exp * kotlin.math.sin(imaginary))
    }

    override fun pow(x: Int): Complex64Value {
        val real = x * kotlin.math.ln(this.abs().value)
        val imaginary = x * kotlin.math.atan2(this.data[1], this.data[0])
        val exp = kotlin.math.exp(real)
        return Complex64Value(exp * kotlin.math.cos(imaginary), exp * kotlin.math.sin(imaginary))
    }

    override fun exp(): Complex64Value {
        val expReal = kotlin.math.exp(this.data[0])
        return Complex64Value(expReal * kotlin.math.cos(this.data[1]), expReal * kotlin.math.sin(this.data[1]))
    }

    override fun ln() = Complex64Value(kotlin.math.ln(this.abs().value), kotlin.math.atan2(this.data[1], this.data[0]))

    override fun sqrt(): Complex64Value = pow(1.0 / 2.0)

    override fun cos(): Complex64Value = Complex64Value(kotlin.math.cos(this.data[0]) * kotlin.math.cosh(this.data[1]), -kotlin.math.sin(this.data[0]) * kotlin.math.sinh(this.data[1]))
    override fun sin(): Complex64Value = Complex64Value(kotlin.math.sin(this.data[0]) * kotlin.math.cosh(this.data[1]), kotlin.math.cos(this.data[0]) * kotlin.math.sinh(this.data[1]))

    override fun tan(): Complex64Value {
        if (this.data[1] > 20.0) {
            return Complex64Value(0.0, 1.0)
        }
        if (this.data[1] < -20.0) {
            return Complex64Value(0.0, -1.0)
        }

        val r: Double = 2.0 * this.data[0]
        val i: Double = 2.0 * this.data[1]
        val d = kotlin.math.cos(r) + kotlin.math.cosh(i)

        return Complex64Value(kotlin.math.sin(r) / d, kotlin.math.sinh(i) / d)
    }

    override fun atan(): Complex64Value = ((this + I) / (I - this)).ln() * (I / Complex32Value(floatArrayOf(2.0f, 0.0f)))
}