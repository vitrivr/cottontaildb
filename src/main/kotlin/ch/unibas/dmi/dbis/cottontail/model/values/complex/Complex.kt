package ch.unibas.dmi.dbis.cottontail.model.values.complex

import kotlin.math.sqrt

/**
 * Represents a complex number with single-precision 32-bit IEEE 754 floating point numbers.
 *
 * Inspired by:
 *
 *    - https://gist.github.com/simonvar/97ab9745e719c34772db4a10a8515d2c
 *    - https://github.com/abdulfatir/jcomplexnumber/blob/master/com/abdulfatir/jcomplexnumber/ComplexNumber.java
 *    - https://introcs.cs.princeton.edu/java/97data/Complex.java.html
 *    - https://rosettacode.org/wiki/Arithmetic/Complex#Kotlin
 *
 * @author Manuel Huerbin
 * @version 1.0
 */
class Complex(private val real: Float = 0.0f, private val imaginary: Float = 0.0f) {

    /**
     * Compares this value with the specified value for order.
     * Returns zero if this value is equal to the specified other value, a negative number if it's less than other,
     * or a positive number if it's greater than other.
     */
    operator fun compareTo(other: Complex): Int {
        return (modulo() - other.modulo()).toInt()
    }

    /** Adds the other value to this value. */
    operator fun plus(other: Complex) = Complex(real + other.real, imaginary + other.imaginary)

    /** Subtracts the other value from this value. */
    operator fun minus(other: Complex) = Complex(real - other.real, imaginary - other.imaginary)

    /** Multiplies this value by the other value. */
    operator fun times(other: Complex) = Complex(real * other.real - imaginary * other.imaginary, real * other.imaginary + other.real * imaginary)

    /** Divides this value by the other value. */
    operator fun div(other: Complex) = this * other.inverse()

    private fun inverse(): Complex {
        return Complex(real / (real * real + imaginary * imaginary), -imaginary / (real * real + imaginary * imaginary))
    }

    /** Modulus of this value (float) */
    private fun modulo(): Float {
        return sqrt((real * real + imaginary * imaginary).toDouble()).toFloat()
    }
}