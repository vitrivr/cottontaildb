package org.vitrivr.cottontail.model.values

import org.vitrivr.cottontail.model.values.types.NumericValue
import org.vitrivr.cottontail.model.values.types.RealVectorValue
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.model.values.types.VectorValue
import java.util.*
import kotlin.math.absoluteValue
import kotlin.math.pow

/**
 * This is an abstraction over a [FloatArray] and it represents a vector of [Float]s.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
inline class FloatVectorValue(val data: FloatArray) : RealVectorValue<Float> {

    companion object {
        /**
         * Generates a [Complex32VectorValue] of the given size initialized with random numbers.
         *
         * @param size Size of the new [Complex32VectorValue]
         * @param rnd A [SplittableRandom] to generate the random numbers.
         */
        fun random(size: Int, rnd: SplittableRandom = SplittableRandom(System.currentTimeMillis())) = FloatVectorValue(FloatArray(size) { Float.fromBits(rnd.nextInt()) })

        /**
         * Generates a [Complex32VectorValue] of the given size initialized with ones.
         *
         * @param size Size of the new [Complex32VectorValue]
         */
        fun one(size: Int) = FloatVectorValue(FloatArray(size) { 1.0f })

        /**
         * Generates a [Complex32VectorValue] of the given size initialized with zeros.
         *
         * @param size Size of the new [Complex32VectorValue]
         * @param rnd A [SplittableRandom] to generate the random numbers.
         */
        fun zero(size: Int) =FloatVectorValue(FloatArray(size))
    }

    constructor(input: List<Number>) : this(FloatArray(input.size) { input[it].toFloat() })
    constructor(input: Array<Number>) : this(FloatArray(input.size) { input[it].toFloat() })

    override val logicalSize: Int
        get() = this.data.size

    override fun compareTo(other: Value): Int {
        throw IllegalArgumentException("FloatVectorValues can can only be compared for equality.")
    }

    /**
     * Returns the indices of this [FloatVectorValue].
     *
     * @return The indices of this [FloatVectorValue]
     */
    override val indices: IntRange
        get() = this.data.indices

    /**
     * Returns the i-th entry of  this [FloatVectorValue].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun get(i: Int): FloatValue = FloatValue(this.data[i])

    /**
     * Returns the i-th entry of  this [FloatVectorValue] as [Boolean].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsBool(i: Int) = this.data[i] != 0.0f

    /**
     * Returns true, if this [FloatVectorValue] consists of all zeroes, i.e. [0, 0, ... 0]
     *
     * @return True, if this [FloatVectorValue] consists of all zeroes
     */
    override fun allZeros(): Boolean = this.data.all { it == 0.0f }

    /**
     * Returns true, if this [FloatVectorValue] consists of all ones, i.e. [1, 1, ... 1]
     *
     * @return True, if this [FloatVectorValue] consists of all ones
     */
    override fun allOnes(): Boolean = this.data.all { it == 1.0f }

    /**
     * Creates and returns a copy of this [FloatVectorValue].
     *
     * @return Exact copy of this [FloatVectorValue].
     */
    override fun copy(): FloatVectorValue = FloatVectorValue(this.data.copyOf(this.data.size))

    override fun plus(other: VectorValue<*>) = if (other is FloatVectorValue) {
        FloatVectorValue(FloatArray(this.data.size) {
            (this.data[it] + other.data[it])
        })
    } else {
        FloatVectorValue(FloatArray(this.data.size) {
            (this.data[it] + other[it].asFloat().value)
        })
    }


    override fun minus(other: VectorValue<*>) = if (other is FloatVectorValue) {
        FloatVectorValue(FloatArray(this.data.size) {
            (this.data[it] - other.data[it])
        })
    } else {
        FloatVectorValue(FloatArray(this.data.size) {
            (this.data[it] - other[it].asFloat().value)
        })
    }

    override fun times(other: VectorValue<*>) = if (other is FloatVectorValue) {
        FloatVectorValue(FloatArray(this.data.size) {
            (this.data[it] * other.data[it])
        })
    } else {
        FloatVectorValue(FloatArray(this.data.size) {
            (this.data[it] * other[it].asFloat().value)
        })
    }

    override fun div(other: VectorValue<*>) = if (other is FloatVectorValue) {
        FloatVectorValue(FloatArray(this.data.size) {
            (this.data[it] / other.data[it])
        })
    } else {
        FloatVectorValue(FloatArray(this.data.size) {
            (this.data[it] / other[it].asFloat().value)
        })
    }

    override fun plus(other: NumericValue<*>): FloatVectorValue {
        val otherAsFloat = other.asFloat().value
        return FloatVectorValue(FloatArray(this.logicalSize) {
            (this.data[it] + otherAsFloat)
        })
    }

    override fun minus(other: NumericValue<*>): FloatVectorValue {
        val otherAsFloat = other.asFloat().value
        return FloatVectorValue(FloatArray(this.logicalSize) {
            (this.data[it] - otherAsFloat)
        })
    }

    override fun times(other: NumericValue<*>): FloatVectorValue {
        val otherAsFloat = other.asFloat().value
        return FloatVectorValue(FloatArray(this.logicalSize) {
            (this.data[it] * otherAsFloat)
        })
    }

    override fun div(other: NumericValue<*>): FloatVectorValue {
        val otherAsFloat = other.asFloat().value
        return FloatVectorValue(FloatArray(this.logicalSize) {
            (this.data[it] / otherAsFloat)
        })
    }

    override fun pow(x: Int) = DoubleVectorValue(DoubleArray(this.data.size) {
        this.data[it].pow(x).toDouble()
    })

    override fun sqrt() = DoubleVectorValue(DoubleArray(this.data.size) {
        kotlin.math.sqrt(this.data[it]).toDouble()
    })

    override fun abs() = FloatVectorValue(FloatArray(this.data.size) {
        kotlin.math.abs(this.data[it])
    })

    override fun sum(): FloatValue = FloatValue(this.data.sum())

    override fun norm2(): FloatValue {
        var sum = 0.0f
        for (i in this.indices) {
            sum += this.data[i].pow(2)
        }
        return FloatValue(kotlin.math.sqrt(sum))
    }

    override fun dot(other: VectorValue<*>): FloatValue {
        var sum = 0.0f
        if (other is FloatVectorValue) {
            for (i in this.indices) {
                sum += other.data[i] * this.data[i]
            }
        } else {
            for (i in this.indices) {
                sum += other[i].value.toFloat() * this.data[i]
            }
        }
        return FloatValue(sum)
    }

    override fun l1(other: VectorValue<*>): FloatValue {
        var sum = 0.0f
        if (other is FloatVectorValue) {
            for (i in this.indices) {
                sum += (other.data[i] - this.data[i]).absoluteValue
            }
        } else {
            for (i in this.indices) {
                sum += (other[i].asFloat().value - this.data[i]).absoluteValue
            }
        }
        return FloatValue(sum)
    }

    override fun l2(other: VectorValue<*>): FloatValue {
        var sum = 0.0f
        if (other is FloatVectorValue) {
            for (i in this.indices) {
                sum += (other.data[i] - this.data[i]).pow(2)
            }
        } else {
            for (i in this.indices) {
                sum += (other[i].asFloat().value - this.data[i]).absoluteValue
            }
        }
        return FloatValue(kotlin.math.sqrt(sum))
    }

    override fun lp(other: VectorValue<*>, p: Int): FloatValue {
        var sum = 0.0f
        if (other is FloatVectorValue) {
            for (i in this.indices) {
                sum += (other.data[i] - this.data[i]).pow(p)
            }
        } else {
            for (i in this.indices) {
                sum += (other[i].value.toFloat() - this.data[i]).pow(p)
            }
        }
        return FloatValue(sum.pow(1.0f/p))
    }
}