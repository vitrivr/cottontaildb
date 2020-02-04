package ch.unibas.dmi.dbis.cottontail.model.values

import kotlin.math.sqrt

inline class Complex64Value(override val value: DoubleArray) : Value<DoubleArray> {

    /**
     * Recommended constructor for [Complex64Value].
     *
     * @param real The real part of the [Complex64Value].
     * @param imaginary The imaginary part of the [Complex64Value].
     */
    constructor(real: Double, imaginary: Double) : this(doubleArrayOf(real, imaginary))

    override val size: Int
        get() = -1

    override val numeric: Boolean
        get() = true

    override fun compareTo(other: Value<*>): Int = when (other) {
        is Complex64Value -> (modulo() - other.modulo()).toInt()
        else -> throw IllegalArgumentException("ComplexValues can only be compared to other ComplexValues.")
    }

    /**
     * Calculates and returns the inverse of this [Complex64Value].
     *
     * @return The inverse [Complex64Value].
     */
    fun inverse(): Complex64Value = Complex64Value(doubleArrayOf(this.value[0] / (this.value[0] * this.value[0] + this.value[1] * this.value[1]), -this.value[1] / (this.value[0] * this.value[0] + this.value[1] * this.value[1])))

    /**
     * Calculates and returns the modulo of this [Complex64Value].
     *
     * @return The module of this [Complex64Value].
     */
    fun modulo(): Double = sqrt((this.value[0] * this.value[0] + this.value[1] * this.value[1]))
}