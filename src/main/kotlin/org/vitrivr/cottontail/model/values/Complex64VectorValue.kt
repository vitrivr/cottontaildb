package org.vitrivr.cottontail.model.values

import org.vitrivr.cottontail.model.values.types.ComplexVectorValue
import org.vitrivr.cottontail.model.values.types.NumericValue
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.model.values.types.VectorValue
import java.util.*

/**
 * This is an abstraction over an [Array] and it represents a vector of [Complex64]s.
 *
 * @author Manuel Huerbin & Ralph Gasser
 * @version 1.2
 */
inline class Complex64VectorValue(val data: DoubleArray) : ComplexVectorValue<Double> {
    /**
     * Constructor given an Array of [Complex32Value]s
     *
     * @param value Array of [Complex32Value]s
     */
    constructor(value: Array<Complex32Value>) : this(DoubleArray(2 * value.size) {
        if (it % 2 == 0) {
            value[it / 2].real.value.toDouble()
        } else {
            value[it / 2].imaginary.value.toDouble()
        }
    })

    /**
     * Constructor given an Array of [Complex64Value]s
     *
     * @param value Array of [Complex64Value]s
     */
    constructor(value: Array<Complex64Value>) : this(DoubleArray(2 * value.size) {
        if (it % 2 == 0) {
            value[it / 2].real.value
        } else {
            value[it / 2].imaginary.value
        }
    })

    companion object {
        /**
         * Generates a [Complex32VectorValue] of the given size initialized with random numbers.
         *
         * @param size Size of the new [Complex32VectorValue]
         * @param rnd A [SplittableRandom] to generate the random numbers.
         */
        fun random(size: Int, rnd: SplittableRandom = SplittableRandom(System.currentTimeMillis())) = Complex64VectorValue(DoubleArray(2 * size) {
            rnd.nextDouble()
        })

        /**
         * Generates a [Complex32VectorValue] of the given size initialized with ones.
         *
         * @param size Size of the new [Complex32VectorValue]
         */
        fun one(size: Int) = Complex64VectorValue(DoubleArray(2 * size) {
            1.0
        })

        /**
         * Generates a [Complex32VectorValue] of the given size initialized with zeros.
         *
         * @param size Size of the new [Complex32VectorValue]
         * @param rnd A [SplittableRandom] to generate the random numbers.
         */
        fun zero(size: Int) = Complex64VectorValue(DoubleArray(2 * size) {
            0.0
        })
    }

    /** Logical size of the [Complex64VectorValue]. */
    override val logicalSize: Int
        get() = this.data.size / 2

    /**
     * Returns the i-th entry of  this [Complex64VectorValue]. All entries with i % 2 == 0 correspond
     * to the real part of the value, whereas entries with i % 2 == 1 correspond to the imaginary part.
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun get(i: Int) = Complex64Value(this.data[i shl 1], this.data[(i shl 1) + 1])
    override fun real(i: Int) = DoubleValue(this.data[i shl 1])
    override fun imaginary(i: Int) = DoubleValue(this.data[(i shl 1) + 1])

    override fun compareTo(other: Value): Int {
        throw IllegalArgumentException("ComplexVectorValues can can only be compared for equality.")
    }

    /**
     * Returns the indices of this [Complex64VectorValue].
     *
     * @return The indices of this [Complex64VectorValue]
     */
    override val indices: IntRange
        get() = (0 until this.logicalSize)

    /**
     * Returns the i-th entry of  this [Complex64VectorValue] as [Boolean]. All entries with index % 2 == 0 correspond
     * to the real part of the value, whereas entries with i % 2 == 1 correspond to the imaginary part.
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsBool(i: Int): Boolean = this[i] != Complex64Value.ZERO

    /**
     * Returns true, if this [Complex64VectorValue] consists of all zeroes, i.e. [0, 0, ... 0]
     *
     * @return True, if this [Complex64VectorValue] consists of all zeroes
     */
    override fun allZeros(): Boolean = this.data.all { it == 0.0 }

    /**
     * Returns true, if this [Complex64VectorValue] consists of all ones, i.e. [1, 1, ... 1]
     *
     * @return True, if this [Complex64VectorValue] consists of all ones
     */
    override fun allOnes(): Boolean = this.data.all { it == 1.0 }

    /**
     * Creates and returns a copy of this [Complex64VectorValue].
     *
     * @return Exact copy of this [Complex64VectorValue].
     */
    override fun copy(): Complex64VectorValue = Complex64VectorValue(this.data.copyOf())

    override fun plus(other: VectorValue<*>) = Complex64VectorValue(when (other) {
        is Complex32VectorValue -> DoubleArray(this.data.size) { this.data[it] + other.data[it] }
        is Complex64VectorValue -> DoubleArray(this.data.size) { this.data[it] + other.data[it] }
        else -> DoubleArray(this.data.size) {
            if (it % 2 == 0) {
                this.data[it] + other[it].value.toDouble()
            } else {
                this.data[it]
            }
        }
    })

    override fun minus(other: VectorValue<*>) = Complex64VectorValue(when (other) {
        is Complex32VectorValue -> DoubleArray(this.data.size) { this.data[it] - other.data[it] }
        is Complex64VectorValue -> DoubleArray(this.data.size) { this.data[it] - other.data[it] }
        else -> DoubleArray(this.data.size) {
            if (it % 2 == 0) {
                this.data[it] - other[it].value.toDouble()
            } else {
                this.data[it]
            }
        }
    })

    override fun times(other: VectorValue<*>) = Complex64VectorValue(when (other) {
        is Complex32VectorValue -> DoubleArray(this.data.size) {
            if (it % 2 == 0) {
                this.data[it] * other.data[it] - this.data[it + 1] * other.data[it + 1]
            } else {
                this.data[it - 1] * other.data[it] + this.data[it] * other.data[it - 1]
            }
        }
        is Complex64VectorValue -> DoubleArray(this.data.size) {
            if (it % 2 == 0) {
                this.data[it] * other.data[it] - this.data[it + 1] * other.data[it + 1]
            } else {
                this.data[it - 1] * other.data[it] + this.data[it] * other.data[it - 1]
            }
        }
        else -> DoubleArray(this.data.size) { this.data[it] * other[it].value.toDouble() }
    })

    override fun div(other: VectorValue<*>) = when (other) {
        is Complex64VectorValue -> internalComplex64VectorValueDiv(other)
        is Complex32VectorValue -> internalComplex32VectorValueDiv(other)
        else -> Complex64VectorValue(DoubleArray(this.data.size) { this.data[it] / other[it].value.toDouble() })
    }

    /**
     * Internal division implementations for [Complex64VectorValue]s.
     */
    private fun internalComplex64VectorValueDiv(other: Complex64VectorValue): Complex64VectorValue {
        val doubles = DoubleArray(this.data.size)
        for (i in 0 until this.data.size / 2) {
            val c = other.data[i shl 1]
            val d = other.data[(i shl 1) + 1]
            if (kotlin.math.abs(c) < kotlin.math.abs(d)) {
                val q = c / d
                val denominator = c * q + d
                doubles[i shl 1] = (this.data[i shl 1] * q + this.data[(i shl 1) + 1]) / denominator
                doubles[(i shl 1) + 1] = (this.data[(i shl 1) + 1] * q - this.data[i shl 1]) / denominator
            } else {
                val q = d / c
                val denominator = d * q + c
                doubles[i shl 1] = (this.data[(i shl 1) + 1] * q + this.data[(i shl 1)]) / denominator
                doubles[(i shl 1) + 1] = (this.data[(i shl 1) + 1] - this.data[i shl 1] * q) / denominator
            }
        }
        return Complex64VectorValue(doubles)
    }

    /**
     * Internal division implementations for [Complex32VectorValue]s.
     */
    private fun internalComplex32VectorValueDiv(other: Complex32VectorValue): Complex64VectorValue {
        val doubles = DoubleArray(this.data.size)
        for (i in 0 until this.data.size / 2) {
            val c = other.data[i shl 1]
            val d = other.data[i shl 1 + 1]
            if (kotlin.math.abs(c) < kotlin.math.abs(d)) {
                val q = c / d
                val denominator = c * q + d
                doubles[i shl 1] = (this.data[i shl 1] * q + this.data[(i shl 1) + 1]) / denominator
                doubles[(i shl 1) + 1] = (this.data[(i shl 1) + 1] * q - this.data[i shl 1]) / denominator
            } else {
                val q = d / c
                val denominator = d * q + c
                doubles[i shl 1] = (this.data[(i shl 1) + 1] * q + this.data[(i shl 1)]) / denominator
                doubles[(i shl 1) + 1] = (this.data[(i shl 1) + 1] * q - this.data[i shl 1]) / denominator
            }
        }
        return Complex64VectorValue(doubles)
    }

    override fun plus(other: NumericValue<*>) = Complex64VectorValue(when (other) {
        is Complex32Value -> DoubleArray(this.data.size) {
            if (it % 2 == 0) {
                this.data[it] + other.data[0]
            } else {
                this.data[it] + other.data[1]
            }
        }
        is Complex64Value -> DoubleArray(this.data.size) {
            if (it % 2 == 0) {
                this.data[it] + other.data[0]
            } else {
                this.data[it] + other.data[1]
            }
        }
        else -> DoubleArray(this.data.size) {
            if (it % 2 == 0) {
                this.data[it] + other.value.toDouble()
            } else {
                this.data[it]
            }
        }
    })

    override fun minus(other: NumericValue<*>) = Complex64VectorValue(when (other) {
        is Complex32Value -> DoubleArray(this.data.size) {
            if (it % 2 == 0) {
                this.data[it] - other.data[0]
            } else {
                this.data[it] - other.data[1]
            }
        }
        is Complex64Value -> DoubleArray(this.data.size) {
            if (it % 2 == 0) {
                this.data[it] - other.data[0]
            } else {
                this.data[it] - other.data[1]
            }
        }
        else -> DoubleArray(this.data.size) {
            if (it % 2 == 0) {
                this.data[it] - other.value.toDouble()
            } else {
                this.data[it]
            }
        }
    })


    override fun times(other: NumericValue<*>) = Complex64VectorValue(when (other) {
        is Complex32Value -> DoubleArray(this.data.size) {
            if (it % 2 == 0) {
                this.data[it] * other.data[0] - this.data[it + 1] * other.data[1]
            } else {
                this.data[it - 1] * other.data[1] + this.data[it] * other.data[0]
            }
        }
        is Complex64Value -> DoubleArray(this.data.size) {
            if (it % 2 == 0) {
                this.data[it] * other.data[0] - this.data[it + 1] * other.data[1]
            } else {
                this.data[it - 1] * other.data[1] + this.data[it] * other.data[0]
            }
        }
        else -> DoubleArray(this.data.size) { this.data[it] * other.value.toDouble() }
    })

    override fun div(other: NumericValue<*>) = when (other) {
        is Complex64Value -> this.internalComplex64ValueDiv(other)
        is Complex32Value -> this.internalComplex32ValueDiv(other)
        else -> Complex64VectorValue(DoubleArray(this.data.size) { this.data[it] / other.value.toDouble() })
    }

    /**
     * Internal division implementations for [Complex64Value]s.
     */
    private fun internalComplex64ValueDiv(other: Complex64Value): Complex64VectorValue {
        val doubles = DoubleArray(this.data.size)
        for (i in 0 until this.data.size / 2) {
            val c = other.data[0]
            val d = other.data[1]
            if (kotlin.math.abs(c) < kotlin.math.abs(d)) {
                val q = c / d
                val denominator = c * q + d
                doubles[i shl 1] = (this.data[i shl 1] * q + this.data[(i shl 1) + 1]) / denominator
                doubles[(i shl 1) + 1] = (this.data[(i shl 1) + 1] * q - this.data[i shl 1]) / denominator
            } else {
                val q = d / c
                val denominator = d * q + c
                doubles[i shl 1] = (this.data[(i shl 1) + 1] * q + this.data[(i shl 1)]) / denominator
                doubles[(i shl 1) + 1] = (this.data[(i shl 1) + 1] - this.data[i shl 1] * q) / denominator
            }
        }
        return Complex64VectorValue(doubles)
    }

    /**
     * Internal division implementations for [Complex32Value]s.
     */
    private fun internalComplex32ValueDiv(other: Complex32Value): Complex64VectorValue {
        val doubles = DoubleArray(this.data.size)
        for (i in 0 until this.data.size / 2) {
            val c = other.data[0]
            val d = other.data[1]
            if (kotlin.math.abs(c) < kotlin.math.abs(d)) {
                val q = c / d
                val denominator = c * q + d
                doubles[i shl 1] = (this.data[i shl 1] * q + this.data[(i shl 1) + 1]) / denominator
                doubles[(i shl 1) + 1] = (this.data[(i shl 1) + 1] * q - this.data[i shl 1]) / denominator
            } else {
                val q = d / c
                val denominator = d * q + c
                doubles[i shl 1] = (this.data[(i shl 1) + 1] * q + this.data[(i shl 1)]) / denominator
                doubles[(i shl 1) + 1] = (this.data[(i shl 1) + 1] - this.data[i shl 1] * q) / denominator
            }
        }
        return Complex64VectorValue(doubles)
    }

    override fun pow(x: Int) = Complex64VectorValue(DoubleArray(this.data.size) {
        TODO()
    })

    override fun sqrt() = Complex64VectorValue(DoubleArray(this.data.size) {
        TODO()
    })

    override fun abs() = Complex64VectorValue(DoubleArray(this.data.size) { kotlin.math.abs(this.data[it]) })

    override fun sum(): Complex64Value {
        var real = 0.0
        var imaginary = 0.0
        this.data.forEachIndexed { i, d ->
            if (i % 2 == 0) {
                real += d
            } else {
                imaginary += d
            }
        }
        return Complex64Value(real, imaginary)
    }

    override fun norm2(): Complex64Value {
        TODO()
    }

    override fun dot(other: VectorValue<*>): Complex64Value {
        TODO()
    }

    override fun l1(other: VectorValue<*>): Complex64Value {
        TODO()
    }

    override fun l2(other: VectorValue<*>): Complex64Value {
        TODO()
    }

    override fun lp(other: VectorValue<*>, p: Int): Complex64Value {
        TODO()
    }
}