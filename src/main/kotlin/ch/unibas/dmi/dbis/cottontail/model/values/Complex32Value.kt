package ch.unibas.dmi.dbis.cottontail.model.values

import kotlin.math.sqrt

inline class Complex32Value(override val value: FloatArray) : Value<FloatArray> {

    /**
     * Recommended constructor for [Complex32Value].
     *
     * @param real The real part of the [Complex32Value].
     * @param imaginary The imaginary part of the [Complex32Value].
     */
    constructor(real: Float, imaginary: Float) : this(floatArrayOf(real, imaginary))

    override val size: Int
        get() = -1

    override val numeric: Boolean
        get() = true

    override fun compareTo(other: Value<*>): Int = when (other) {
        is Complex32Value -> (modulo() - other.modulo()).toInt()
        else -> throw IllegalArgumentException("ComplexValues can only be compared to other ComplexValues.")
    }

    /**
     * Calculates and returns the inverse of this [Complex32Value].
     *
     * @return The inverse [Complex32Value].
     */
    fun inverse(): Complex32Value = Complex32Value(floatArrayOf(this.value[0] / (this.value[0] * this.value[0] + this.value[1] * this.value[1]), -this.value[1] / (this.value[0] * this.value[0] + this.value[1] * this.value[1])))

    /**
     * Calculates and returns the modulo of this [Complex32Value].
     *
     * @return The module of this [Complex32Value].
     */
    fun modulo(): Float = sqrt((this.value[0] * this.value[0] + this.value[1] * this.value[1]))
}