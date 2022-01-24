package org.vitrivr.cottontail.core.values

import org.vitrivr.cottontail.core.values.types.*
import org.vitrivr.cottontail.utilities.extensions.toInt
import java.util.*

/**
 * This is an abstraction over a [BooleanArray] and it represents a vector of [Boolean]s.
 *
 * @author Ralph Gasser
 * @version 1.6.0
 */
@JvmInline
value class BooleanVectorValue(val data: BooleanArray) : RealVectorValue<Int> {

    companion object {
        /**
         * Generates a [IntVectorValue] of the given size initialized with random numbers.
         *
         * @param size Size of the new [IntVectorValue]
         * @param rnd A [SplittableRandom] to generate the random numbers.
         * @return Random [BooleanVectorValue] of size [size]
         */
        fun random(size: Int, rnd: SplittableRandom = Value.RANDOM) =
            org.vitrivr.cottontail.core.values.BooleanVectorValue(BooleanArray(size) { rnd.nextBoolean() })

        /**
         * Generates a [IntVectorValue] of the given size initialized with ones.
         *
         * @param size Size of the new [IntVectorValue]
         */
        fun one(size: Int) = org.vitrivr.cottontail.core.values.BooleanVectorValue(BooleanArray(size) { true })

        /**
         * Generates a [IntVectorValue] of the given size initialized with zeros.
         *
         * @param size Size of the new [IntVectorValue]
         */
        fun zero(size: Int) = org.vitrivr.cottontail.core.values.BooleanVectorValue(BooleanArray(size))
    }

    constructor(input: List<Number>) : this(BooleanArray(input.size) { input[it].toInt() == 1 })
    constructor(input: Array<Number>) : this(BooleanArray(input.size) { input[it].toInt() == 1 })
    constructor(input: Array<Boolean>) : this(BooleanArray(input.size) { input[it] })

    /** The logical size of this [BooleanVectorValue]. */
    override val logicalSize: Int
        get() = this.data.size

    /** The [Types] size of this [BooleanVectorValue]. */
    override val type: Types<*>
        get() = Types.BooleanVector(this.logicalSize)

    /**
     * Checks for equality between this [BooleanVectorValue] and the other [Value]. Equality can only be
     * established if the other [Value] is also a [BooleanVectorValue] and holds the same value.
     *
     * @param other [Value] to compare to.
     * @return True if equal, false otherwise.
     */
    override fun isEqual(other: Value): Boolean = (other is org.vitrivr.cottontail.core.values.BooleanVectorValue) && (this.data.contentEquals(other.data))

    /**
     * Returns the indices of this [BooleanVectorValue].
     *
     * @return The indices of this [BooleanVectorValue]
     */
    override val indices: IntRange
        get() = this.data.indices

    /**
     * Returns the i-th entry of this [BooleanVectorValue].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun get(i: Int): org.vitrivr.cottontail.core.values.IntValue = IntValue(this.data[i].toInt())

    /**
     * Returns a sub vector of this [BooleanVectorValue] starting at the component [start] and
     * containing [length] components.
     *
     * @param start Index of the first entry of the returned vector.
     * @param length how many elements, including start, to return
     *
     * @return The [BooleanVectorValue] representing the sub-vector.
     */
    override fun subvector(start: Int, length: Int) =
        org.vitrivr.cottontail.core.values.BooleanVectorValue(this.data.copyOfRange(start, start + length))

    /**
     * Returns the i-th entry of  this [BooleanVectorValue] as [Boolean].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsBool(i: Int) = this.data[i]

    /**
     * Returns true, if this [BooleanVectorValue] consists of all zeroes, i.e. [0, 0, ... 0]
     *
     * @return True, if this [BooleanVectorValue] consists of all zeroes
     */
    override fun allZeros(): Boolean = this.indices.all { !this.data[it] }

    /**
     * Returns true, if this [BooleanVectorValue] consists of all ones, i.e. [1, 1, ... 1]
     *
     * @return True, if this [BooleanVectorValue] consists of all ones
     */
    override fun allOnes(): Boolean = this.indices.all { this.data[it] }

    /**
     * Creates and returns a copy of this [BooleanVectorValue].
     *
     * @return Exact copy of this [BooleanVectorValue].
     */
    override fun copy(): org.vitrivr.cottontail.core.values.BooleanVectorValue =
        org.vitrivr.cottontail.core.values.BooleanVectorValue(this.data.copyOf())

    /**
     * Creates and returns a new instance of [BooleanVectorValue] of the same size.
     *
     * @return New instance of [BooleanVectorValue]
     */
    override fun new(): org.vitrivr.cottontail.core.values.BooleanVectorValue =
        org.vitrivr.cottontail.core.values.BooleanVectorValue(BooleanArray(this.data.size))

    override fun plus(other: VectorValue<*>): VectorValue<Int> = when (other) {
        is org.vitrivr.cottontail.core.values.BooleanVectorValue -> org.vitrivr.cottontail.core.values.IntVectorValue(
            IntArray(this.data.size) {
                (this.data[it].toInt() + other.data[it].toInt())
            })
        is org.vitrivr.cottontail.core.values.IntVectorValue -> org.vitrivr.cottontail.core.values.IntVectorValue(
            IntArray(this.data.size) {
                (this.data[it].toInt() + other.data[it])
            })
        else -> org.vitrivr.cottontail.core.values.IntVectorValue(IntArray(this.data.size) {
            (this.data[it].toInt() + other[it].asInt().value)
        })
    }

    override fun minus(other: VectorValue<*>): VectorValue<Int> = when (other) {
        is org.vitrivr.cottontail.core.values.BooleanVectorValue -> org.vitrivr.cottontail.core.values.IntVectorValue(
            IntArray(this.data.size) {
                (this.data[it].toInt() - other.data[it].toInt())
            })
        is org.vitrivr.cottontail.core.values.IntVectorValue -> org.vitrivr.cottontail.core.values.IntVectorValue(
            IntArray(this.data.size) {
                (this.data[it].toInt() - other.data[it])
            })
        else -> org.vitrivr.cottontail.core.values.IntVectorValue(IntArray(this.data.size) {
            (this.data[it].toInt() - other[it].asInt().value)
        })
    }

    override fun times(other: VectorValue<*>): VectorValue<Int> = when (other) {
        is org.vitrivr.cottontail.core.values.BooleanVectorValue -> org.vitrivr.cottontail.core.values.IntVectorValue(
            IntArray(this.data.size) {
                (this.data[it].toInt() * other.data[it].toInt())
            })
        is org.vitrivr.cottontail.core.values.IntVectorValue -> org.vitrivr.cottontail.core.values.IntVectorValue(
            IntArray(this.data.size) {
                (this.data[it].toInt() * other.data[it])
            })
        else -> org.vitrivr.cottontail.core.values.IntVectorValue(IntArray(this.data.size) {
            (this.data[it].toInt() * other[it].asInt().value)
        })
    }

    override fun div(other: VectorValue<*>): VectorValue<Int> = when (other) {
        is org.vitrivr.cottontail.core.values.BooleanVectorValue -> org.vitrivr.cottontail.core.values.IntVectorValue(
            IntArray(this.data.size) {
                (this.data[it].toInt() / other.data[it].toInt())
            })
        is org.vitrivr.cottontail.core.values.IntVectorValue -> org.vitrivr.cottontail.core.values.IntVectorValue(
            IntArray(this.data.size) {
                (this.data[it].toInt() / other.data[it])
            })
        else -> org.vitrivr.cottontail.core.values.IntVectorValue(IntArray(this.data.size) {
            (this.data[it].toInt() / other[it].asInt().value)
        })
    }

    override fun plus(other: NumericValue<*>): VectorValue<Int> =
        org.vitrivr.cottontail.core.values.IntVectorValue(IntArray(this.logicalSize) {
            (this.data[it].toInt() + other.asInt().value)
        })

    override fun minus(other: NumericValue<*>): VectorValue<Int> =
        org.vitrivr.cottontail.core.values.IntVectorValue(IntArray(this.logicalSize) {
            (this.data[it].toInt() - other.asInt().value)
        })

    override fun times(other: NumericValue<*>): VectorValue<Int> =
        org.vitrivr.cottontail.core.values.IntVectorValue(IntArray(this.logicalSize) {
            (this.data[it].toInt() * other.asInt().value)
        })

    override fun div(other: NumericValue<*>): VectorValue<Int> =
        org.vitrivr.cottontail.core.values.IntVectorValue(IntArray(this.logicalSize) {
            (this.data[it].toInt() / other.asInt().value)
        })

    override fun pow(x: Int): org.vitrivr.cottontail.core.values.DoubleVectorValue = if (x == 0) {
        org.vitrivr.cottontail.core.values.DoubleVectorValue.Companion.one(this.data.size)
    } else {
        org.vitrivr.cottontail.core.values.DoubleVectorValue(DoubleArray(this.data.size) {
            if (this.data[it]) {
                1.0
            } else {
                0.0
            }
        })
    }

    override fun sqrt(): org.vitrivr.cottontail.core.values.DoubleVectorValue =
        org.vitrivr.cottontail.core.values.DoubleVectorValue(DoubleArray(this.data.size) {
            if (this.data[it]) {
                1.0
            } else {
                0.0
            }
        })

    override fun abs(): RealVectorValue<Int> = this.copy()

    override fun sum(): org.vitrivr.cottontail.core.values.DoubleValue = DoubleValue(this.data.sumOf {
        if (it) {
            1.0
        } else {
            0.0
        }
    })

    override fun norm2(): org.vitrivr.cottontail.core.values.DoubleValue {
        var sum = 0.0
        for (i in this.data) {
            if (i) sum += 1.0
        }
        return DoubleValue(kotlin.math.sqrt(sum))
    }

    override fun dot(other: VectorValue<*>): org.vitrivr.cottontail.core.values.DoubleValue = when (other) {
        is org.vitrivr.cottontail.core.values.BooleanVectorValue -> {
            var sum = 0.0
            for (i in this.data.indices) {
                if (this.data[i] && other.data[i]) sum += 1.0
            }
            DoubleValue(sum)
        }
        else -> {
            var sum = 0.0
            for (i in this.data.indices) {
                if (this.data[i]) sum += other[i].asDouble().value
            }
            DoubleValue(sum)
        }
    }

    override fun hamming(other: VectorValue<*>): org.vitrivr.cottontail.core.values.IntValue = when (other) {
        is org.vitrivr.cottontail.core.values.BooleanVectorValue -> {
            var sum = 0
            val start = Arrays.mismatch(this.data, other.data)
            for (i in start until other.data.size) {
                if (this.data[i] != other.data[i]) {
                    sum += 1
                }
            }
            IntValue(sum)
        }
        else -> super.hamming(other).asInt()
    }
}