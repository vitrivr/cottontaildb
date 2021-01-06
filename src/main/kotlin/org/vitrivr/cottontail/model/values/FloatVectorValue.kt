package org.vitrivr.cottontail.model.values

import org.vitrivr.cottontail.model.values.types.NumericValue
import org.vitrivr.cottontail.model.values.types.RealVectorValue
import org.vitrivr.cottontail.model.values.types.Value
import org.vitrivr.cottontail.model.values.types.Value.Companion.RANDOM
import org.vitrivr.cottontail.model.values.types.VectorValue
import org.vitrivr.cottontail.utilities.extensions.nextFloat
import java.util.*
import kotlin.math.absoluteValue
import kotlin.math.pow

/**
 * This is an abstraction over a [FloatArray] and it represents a vector of [Float]s.
 *
 * @author Ralph Gasser
 * @version 1.3.3
 */
inline class FloatVectorValue(val data: FloatArray) : RealVectorValue<Float> {

    companion object {
        /**
         * Generates a [FloatVectorValue] of the given size initialized with random numbers.
         *
         * @param size Size of the new [FloatVectorValue]
         * @param rnd A [SplittableRandom] to generate the random numbers.
         */
        fun random(size: Int, rnd: SplittableRandom = RANDOM) = FloatVectorValue(FloatArray(size) { rnd.nextFloat() })

        /**
         * Generates a [FloatVectorValue] of the given size initialized with ones.
         *
         * @param size Size of the new [FloatVectorValue]
         */
        fun one(size: Int) = FloatVectorValue(FloatArray(size) { 1.0f })

        /**
         * Generates a [FloatVectorValue] of the given size initialized with zeros.
         *
         * @param size Size of the new [FloatVectorValue]
         */
        fun zero(size: Int) = FloatVectorValue(FloatArray(size))
    }

    constructor(input: List<Number>) : this(FloatArray(input.size) { input[it].toFloat() })
    constructor(input: Array<Number>) : this(FloatArray(input.size) { input[it].toFloat() })

    override val logicalSize: Int
        get() = this.data.size

    /**
     * Checks for equality between this [FloatVectorValue] and the other [Value]. Equality can only be
     * established if the other [Value] is also a [FloatVectorValue] and holds the same value.
     *
     * @param other [Value] to compare to.
     * @return True if equal, false otherwise.
     */
    override fun isEqual(other: Value): Boolean = (other is FloatVectorValue) && (this.data.contentEquals(other.data))

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
     * Returns the subvector of length [length] starting from [start] of this [VectorValue].
     *
     * @param start Index of the first entry of the returned vector.
     * @param length how many elements, including start, to return
     * @return The subvector starting at index start containing length elements.
     */
    override fun get(start: Int, length: Int) = FloatVectorValue(FloatArray(length) { data[start + it] })

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

    override fun plus(other: VectorValue<*>) = when (other) {
        is FloatVectorValue -> FloatVectorValue(FloatArray(this.data.size) {
            (this.data[it] + other.data[it])
        })
        else -> FloatVectorValue(FloatArray(this.data.size) {
            (this.data[it] + other[it].asFloat().value)
        })
    }

    override operator fun minus(other: VectorValue<*>) = if (this.logicalSize == other.logicalSize) {
        minus(other, 0, 0, logicalSize)
    } else {
        throw IllegalArgumentException("Dimensions ${this.logicalSize} and ${other.logicalSize} don't agree!")
    }

    /**
     * Calculates the element-wise difference of this and the other [VectorValue]. Subvectors can be defined by the
     * [start] [otherStart] and [length] parameters.
     *
     * @param other The [VectorValue] to subtract from this [VectorValue].
     * @param start the index of the subvector of this to start from
     * @param otherStart the index of the subvector of other to start from
     * @param length the number of elements to build the dot product with from the respective starts
     * @return [VectorValue] that contains the element-wise difference of the two input [VectorValue]s
     */
    override fun minus(other: VectorValue<*>, start: Int, otherStart: Int, length: Int) = when (other) {
        is FloatVectorValue -> FloatVectorValue(FloatArray(length) {
            (this.data[start + it] - other.data[otherStart + it])
        })
        else -> FloatVectorValue(FloatArray(length) {
            (this.data[start + it] - other[otherStart + it].asFloat().value)
        })
    }

    override fun times(other: VectorValue<*>) = when (other) {
        is FloatVectorValue -> FloatVectorValue(FloatArray(this.data.size) {
            (this.data[it] * other.data[it])
        })
        else -> FloatVectorValue(FloatArray(this.data.size) {
            (this.data[it] * other[it].asFloat().value)
        })
    }

    override fun div(other: VectorValue<*>) = when (other) {
        is FloatVectorValue -> FloatVectorValue(FloatArray(this.data.size) {
            (this.data[it] / other.data[it])
        })
        else -> FloatVectorValue(FloatArray(this.data.size) {
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

    override fun pow(x: Int) = FloatVectorValue(FloatArray(this.data.size) {
        this.data[it].pow(x)
    })

    override fun sqrt() = FloatVectorValue(FloatArray(this.data.size) {
        kotlin.math.sqrt(this.data[it])
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

    override fun dot(other: VectorValue<*>): DoubleValue = if (other.logicalSize == this.logicalSize) {
        dot(other, 0, 0, logicalSize)
    } else {
        throw IllegalArgumentException("Dimensions ${this.logicalSize} and ${other.logicalSize} don't agree!")
    }

    /**
     * Builds the dot product between this and the other [VectorValue]. Subvectors can be defined by the
     * [start] [otherStart] and [length] parameters.
     *
     * <strong>Warning:</string> Since the value generated by this function might not fit into the
     * type held by this [VectorValue], the [NumericValue] returned by this function might differ.
     *
     * @param start the index of the sub-vector of this to start from
     * @param otherStart the index of the sub-vector of other to start from
     * @param length the number of elements to build the dot product with from the respective starts
     * @return Sum of the elements of this [VectorValue].
     */
    override fun dot(other: VectorValue<*>, start: Int, otherStart: Int, length: Int) = when (other) {
        is DoubleVectorValue -> {
            var sum = 0.0
            for (i in 0 until length) {
                sum += this.data[start + i] * other.data[otherStart + i]
            }
            DoubleValue(sum)
        }
        is FloatVectorValue -> {
            var sum = 0.0
            for (i in 0 until length) {
                sum += this.data[start + i] * other.data[otherStart + i]
            }
            DoubleValue(sum)
        }
        else -> {
            var sum = 0.0
            for (i in 0 until length) {
                sum += this.data[start + i] * other[otherStart + i].value.toDouble()
            }
            DoubleValue(sum)
        }
    }

    override fun l1(other: VectorValue<*>): DoubleValue = when (other) {
        is FloatVectorValue -> {
            var sum = 0.0
            for (i in this.data.indices) {
                sum += kotlin.math.abs(this.data[i] - other.data[i])
            }
            DoubleValue(sum)
        }
        else -> {
            var sum = 0.0
            for (i in this.data.indices) {
                sum += kotlin.math.abs(this.data[i] - other[i].value.toDouble())
            }
            DoubleValue(sum)
        }
    }

    override fun l2(other: VectorValue<*>): DoubleValue = when (other) {
        is FloatVectorValue -> {
            var sum = 0.0
            for (i in this.data.indices) {
                sum += (this.data[i] - other.data[i]).pow(2)
            }
            DoubleValue(kotlin.math.sqrt(sum))
        }
        else -> {
            var sum = 0.0
            for (i in this.data.indices) {
                sum += (this.data[i] - other[i].value.toDouble()).pow(2)
            }
            DoubleValue(kotlin.math.sqrt(sum))
        }
    }

    override fun lp(other: VectorValue<*>, p: Int) = when (other) {
        is FloatVectorValue -> {
            var sum = 0.0
            for (i in this.data.indices) {
                sum += (this.data[i] - other.data[i]).absoluteValue.pow(p)
            }
            DoubleValue(sum.pow(1.0 / p))
        }
        else -> {
            var sum = 0.0
            for (i in this.data.indices) {
                sum += (this.data[i] - other[i].value.toDouble()).absoluteValue.pow(p)
            }
            DoubleValue(sum.pow(1.0 / p))
        }
    }

    override fun hamming(other: VectorValue<*>): FloatValue = when (other) {
        is FloatVectorValue -> {
            var sum = 0f
            val start = Arrays.mismatch(this.data, other.data)
            for (i in start until other.data.size) {
                if (this.data[i] != other.data[i]) {
                    sum += 1.0f
                }
            }
            FloatValue(sum)
        }
        else -> FloatValue(this.data.size)
    }
}