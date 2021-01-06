package org.vitrivr.cottontail.model.values

import org.vitrivr.cottontail.model.values.types.*
import org.vitrivr.cottontail.model.values.types.Value.Companion.RANDOM
import java.util.*
import kotlin.math.absoluteValue
import kotlin.math.pow

/**
 * This is an abstraction over a [DoubleArray] and it represents a vector of [Double]s.
 *
 * @author Ralph Gasser
 * @version 1.3.3
 */
inline class DoubleVectorValue(val data: DoubleArray) : RealVectorValue<Double> {

    companion object {
        /**
         * Generates a [DoubleVectorValue] of the given size initialized with random numbers.
         *
         * @param size Size of the new [DoubleVectorValue]
         * @param rnd A [SplittableRandom] to generate the random numbers.
         */
        fun random(size: Int, rnd: SplittableRandom = RANDOM) = DoubleVectorValue(DoubleArray(size) { rnd.nextDouble() })

        /**
         * Generates a [Complex32VectorValue] of the given size initialized with ones.
         *
         * @param size Size of the new [DoubleVectorValue]
         */
        fun one(size: Int) = DoubleVectorValue(DoubleArray(size) { 1.0 })

        /**
         * Generates a [DoubleVectorValue] of the given size initialized with zeros.
         *
         * @param size Size of the new [DoubleVectorValue]
         */
        fun zero(size: Int) = DoubleVectorValue(DoubleArray(size))
    }

    constructor(input: List<Number>) : this(DoubleArray(input.size) { input[it].toDouble() })
    constructor(input: Array<Number>) : this(DoubleArray(input.size) { input[it].toDouble() })

    override val logicalSize: Int
        get() = this.data.size

    /**
     * Checks for equality between this [DoubleVectorValue] and the other [Value]. Equality can only be
     * established if the other [Value] is also a [DoubleVectorValue] and holds the same value.
     *
     * @param other [Value] to compare to.
     * @return True if equal, false otherwise.
     */
    override fun isEqual(other: Value): Boolean = (other is DoubleVectorValue) && (this.data.contentEquals(other.data))

    /**
     * Returns the indices of this [DoubleVectorValue].
     *
     * @return The indices of this [DoubleVectorValue]
     */
    override val indices: IntRange
        get() = this.data.indices

    /**
     * Returns the i-th entry of  this [DoubleVectorValue].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun get(i: Int): DoubleValue = DoubleValue(this.data[i])

    /**
     * Returns the subvector of length [length] starting from [start] of this [VectorValue].
     *
     * @param start Index of the first entry of the returned vector.
     * @param length how many elements, including start, to return
     * @return The subvector starting at index start containing length elements.
     */
    override fun get(start: Int, length: Int) = DoubleVectorValue(
        DoubleArray(length) {
            data[start + it]
        }
    )

    /**
     * Returns the i-th entry of  this [DoubleVectorValue] as [Boolean].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsBool(i: Int) = this.data[i] != 0.0

    /**
     * Returns true, if this [DoubleVectorValue] consists of all zeroes, i.e. [0, 0, ... 0]
     *
     * @return True, if this [DoubleVectorValue] consists of all zeroes
     */
    override fun allZeros(): Boolean = this.data.all { it == 0.0 }

    /**
     * Returns true, if this [DoubleVectorValue] consists of all ones, i.e. [1, 1, ... 1]
     *
     * @return True, if this [DoubleVectorValue] consists of all ones
     */
    override fun allOnes(): Boolean = this.data.all { it == 1.0 }

    /**
     * Creates and returns a copy of this [DoubleVectorValue].
     *
     * @return Exact copy of this [DoubleVectorValue].
     */
    override fun copy(): DoubleVectorValue = DoubleVectorValue(this.data.copyOf(this.data.size))

    override fun plus(other: VectorValue<*>) = when (other) {
        is DoubleVectorValue -> DoubleVectorValue(DoubleArray(this.data.size) {
            (this.data[it] + other.data[it])
        })
        else -> DoubleVectorValue(DoubleArray(this.data.size) {
            (this.data[it] + other[it].asDouble().value)
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
    override fun minus(other: VectorValue<*>, start: Int, otherStart: Int, length: Int)  = when (other) {
        is FloatVectorValue -> DoubleVectorValue(DoubleArray(length) {
            (this.data[start + it] - other.data[otherStart + it])
        })
        is DoubleVectorValue -> DoubleVectorValue(DoubleArray(length) {
            (this.data[start + it] - other.data[otherStart + it])
        })
        else -> DoubleVectorValue(DoubleArray(length) {
            (this.data[start + it] - other[otherStart + it].asDouble().value)
        })
    }

    override fun times(other: VectorValue<*>) = when (other) {
        is DoubleVectorValue -> DoubleVectorValue(DoubleArray(this.data.size) {
            (this.data[it] * other.data[it])
        })
        else -> DoubleVectorValue(DoubleArray(this.data.size) {
            (this.data[it] * other[it].asDouble().value)
        })
    }

    override fun div(other: VectorValue<*>) = when (other) {
        is DoubleVectorValue -> DoubleVectorValue(DoubleArray(this.data.size) {
            (this.data[it] / other.data[it])
        })
        else -> DoubleVectorValue(DoubleArray(this.data.size) {
            (this.data[it] / other[it].asDouble().value)
        })
    }

    override fun plus(other: NumericValue<*>): DoubleVectorValue {
        val otherAsDouble = other.asDouble().value
        return DoubleVectorValue(DoubleArray(this.logicalSize) {
            (this.data[it] + otherAsDouble)
        })
    }

    override fun minus(other: NumericValue<*>): DoubleVectorValue {
        val otherAsDouble = other.asDouble().value
        return DoubleVectorValue(DoubleArray(this.logicalSize) {
            (this.data[it] - otherAsDouble)
        })
    }

    override fun times(other: NumericValue<*>): DoubleVectorValue {
        val otherAsDouble = other.asDouble().value
        return DoubleVectorValue(DoubleArray(this.logicalSize) {
            (this.data[it] * otherAsDouble)
        })
    }

    override fun div(other: NumericValue<*>): DoubleVectorValue {
        val otherAsDouble = other.asDouble().value
        return DoubleVectorValue(DoubleArray(this.logicalSize) {
            (this.data[it] / otherAsDouble)
        })
    }

    override fun pow(x: Int) = DoubleVectorValue(DoubleArray(this.data.size) {
        this.data[it].pow(x)
    })

    override fun sqrt() = DoubleVectorValue(DoubleArray(this.data.size) {
        kotlin.math.sqrt(this.data[it])
    })

    override fun abs() = DoubleVectorValue(DoubleArray(this.data.size) {
        kotlin.math.abs(this.data[it])
    })

    override fun sum() = DoubleValue(this.data.sum())

    override fun norm2(): RealValue<*> {
        var sum = 0.0
        for (i in this.data.indices) {
            sum += this.data[i].pow(2)
        }
        return DoubleValue(kotlin.math.sqrt(sum))
    }

    override infix fun dot(other: VectorValue<*>) = if (other.logicalSize == this.logicalSize)
        dot(other, 0, 0, logicalSize)
    else throw IllegalArgumentException("Dimensions ${this.logicalSize} and ${other.logicalSize} don't agree!")

    /**
     * Builds the dot product between this and the other [VectorValue]. Subvectors can be defined by the
     * [start] [otherStart] and [length] parameters.
     *
     * <strong>Warning:</string> Since the value generated by this function might not fit into the
     * type held by this [VectorValue], the [NumericValue] returned by this function might differ.
     *
     * @param start the index of the subvector of this to start from
     * @param otherStart the index of the subvector of other to start from
     * @param length the number of elements to build the dot product with from the respective starts
     * @return Sum of the elements of this [VectorValue].
     */
    override fun dot(other: VectorValue<*>, start: Int, otherStart: Int, length: Int): DoubleValue = when (other) {
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
        is DoubleVectorValue -> {
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
        is DoubleVectorValue -> {
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
        is DoubleVectorValue -> {
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

    override fun hamming(other: VectorValue<*>): DoubleValue = when (other) {
        is DoubleVectorValue -> {
            var sum = 0.0
            val start = Arrays.mismatch(this.data, other.data)
            for (i in start until other.data.size) {
                if (this.data[i] != other.data[i]) {
                    sum += 1.0
                }
            }
            DoubleValue(sum)
        }
        else -> DoubleValue(this.data.size)
    }
}