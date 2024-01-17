package org.vitrivr.cottontail.core.values

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.core.types.*
import org.vitrivr.cottontail.grpc.CottontailGrpc
import java.nio.FloatBuffer
import java.util.*
import kotlin.math.absoluteValue
import kotlin.math.pow

/**
 * This is an abstraction over a [FloatArray] and it represents a vector of [Float]s, which are stored using half-precision
 *
 * @author Luca Rossetto
 * @version 1.1.0
 */
@Serializable
@SerialName("HalfVector")
@JvmInline
value class HalfVectorValue(val data: FloatArray) : RealVectorValue<Float>, PublicValue {
    companion object {
        /**
         * A static helper class to use this [HalfVectorValue] in plain Java.
         *
         * @param array [FloatArray] to create [HalfVectorValue] from
         */
        @JvmStatic
        fun of(array: FloatArray) = HalfVectorValue(array)
    }

    constructor(input: List<Number>) : this(FloatArray(input.size) { input[it].toFloat() })
    constructor(input: Array<Number>) : this(FloatArray(input.size) { input[it].toFloat() })
    constructor(input: DoubleArray) : this(FloatArray(input.size) { input[it].toFloat() })
    constructor(input: LongArray) : this(FloatArray(input.size) { input[it].toFloat() })
    constructor(input: IntArray) : this(FloatArray(input.size) { input[it].toFloat() })
    constructor(input: FloatBuffer) : this(FloatArray(input.remaining()) { input[it] })

    /** The logical size of this [HalfVectorValue]. */
    override val logicalSize: Int
        get() = this.data.size

    /** The [Types] of this [HalfVectorValue]. */
    override val type: Types<*>
        get() = Types.HalfVector(this.logicalSize)

    /**
     * Compares this [HalfVectorValue] to another [HalfVectorValue]. The comparison is done lexicographically.
     *
     * @param other [Value] to compare to.
     * @return True if equal, false otherwise.
     */
    override fun compareTo(other: Value): Int {
        require(other is HalfVectorValue) { "HalfVectorValue can only be compared to another HalfVectorValue. This is a programmer's error!"}
        return Arrays.compare(this.data, other.data)
    }

    /**
     * Checks for equality between this [HalfVectorValue] and the other [Value]. Equality can only be
     * established if the other [Value] is also a [HalfVectorValue] and holds the same value.
     *
     * @param other [Value] to compare to.
     * @return True if equal, false otherwise.
     */
    override fun isEqual(other: Value): Boolean = (other is HalfVectorValue) && (this.data.contentEquals(other.data))

    /**
     * Converts this [HalfVectorValue] to a [CottontailGrpc.Literal] gRCP representation.
     *
     * @return [CottontailGrpc.Literal]
     */
    override fun toGrpc(): CottontailGrpc.Literal
            = CottontailGrpc.Literal.newBuilder().setVectorData(CottontailGrpc.Vector.newBuilder().setHalf(CottontailGrpc.FloatVector.newBuilder().addAllVector(this.map { it.value }))).build()

    /**
     * Returns the indices of this [HalfVectorValue].
     *
     * @return The indices of this [HalfVectorValue]
     */
    override val indices: IntRange
        get() = this.data.indices

    /**
     * Returns the i-th entry of  this [HalfVectorValue].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun get(i: Int): FloatValue = FloatValue(this.data[i])

    /**
     * Returns a sub vector of this [HalfVectorValue] starting at the component [start] and
     * containing [length] components.
     *
     * @param start Index of the first entry of the returned vector.
     * @param length how many elements, including start, to return
     *
     * @return The [HalfVectorValue] representing the sub-vector.
     */
    override fun slice(start: Int, length: Int) = HalfVectorValue(this.data.copyOfRange(start, start + length))

    /**
     * Returns the i-th entry of  this [HalfVectorValue] as [Boolean].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsBool(i: Int) = this.data[i] != 0.0f

    /**
     * Returns true, if this [HalfVectorValue] consists of all zeroes, i.e. [0, 0, ... 0]
     *
     * @return True, if this [HalfVectorValue] consists of all zeroes
     */
    override fun allZeros(): Boolean = this.data.all { it == 0.0f }

    /**
     * Returns true, if this [HalfVectorValue] consists of all ones, i.e. [1, 1, ... 1]
     *
     * @return True, if this [HalfVectorValue] consists of all ones
     */
    override fun allOnes(): Boolean = this.data.all { it == 1.0f }

    /**
     * Creates and returns a copy of this [HalfVectorValue].
     *
     * @return Exact copy of this [HalfVectorValue].
     */
    override fun copy(): HalfVectorValue = HalfVectorValue(this.data.copyOf(this.data.size))

    /**
     * Creates and returns a new instance of [HalfVectorValue] of the same size.
     *
     * @return New instance of [HalfVectorValue]
     */
    override fun new(): HalfVectorValue = HalfVectorValue(FloatArray(this.data.size))

    override fun plus(other: VectorValue<*>) = when (other) {
        is HalfVectorValue -> HalfVectorValue(FloatArray(this.data.size) {
            (this.data[it] + other.data[it])
        })
        else -> HalfVectorValue(FloatArray(this.data.size) {
            (this.data[it] + other[it].asFloat().value)
        })
    }

    override operator fun minus(other: VectorValue<*>) = when (other) {
        is HalfVectorValue -> HalfVectorValue(FloatArray(this.data.size) {
            (this.data[it] - other.data[it])
        })
        else -> HalfVectorValue(FloatArray(this.data.size) {
            (this.data[it] - other[it].asFloat().value)
        })
    }

    override fun times(other: VectorValue<*>) = when (other) {
        is HalfVectorValue -> HalfVectorValue(FloatArray(this.data.size) {
            (this.data[it] * other.data[it])
        })
        else -> HalfVectorValue(FloatArray(this.data.size) {
            (this.data[it] * other[it].asFloat().value)
        })
    }

    override fun div(other: VectorValue<*>) = when (other) {
        is HalfVectorValue -> HalfVectorValue(FloatArray(this.data.size) {
            (this.data[it] / other.data[it])
        })
        else -> HalfVectorValue(FloatArray(this.data.size) {
            (this.data[it] / other[it].asFloat().value)
        })
    }

    override fun plus(other: NumericValue<*>): HalfVectorValue {
        val otherAsFloat = other.asFloat().value
        return HalfVectorValue(FloatArray(this.logicalSize) {
            (this.data[it] + otherAsFloat)
        })
    }

    override fun minus(other: NumericValue<*>): HalfVectorValue {
        val otherAsFloat = other.asFloat().value
        return HalfVectorValue(FloatArray(this.logicalSize) {
            (this.data[it] - otherAsFloat)
        })
    }

    override fun times(other: NumericValue<*>): HalfVectorValue {
        val otherAsFloat = other.asFloat().value
        return HalfVectorValue(FloatArray(this.logicalSize) {
            (this.data[it] * otherAsFloat)
        })
    }

    override fun div(other: NumericValue<*>): HalfVectorValue {
        val otherAsFloat = other.asFloat().value
        return HalfVectorValue(FloatArray(this.logicalSize) {
            (this.data[it] / otherAsFloat)
        })
    }

    override fun pow(x: Int) = HalfVectorValue(FloatArray(this.data.size) {
        this.data[it].pow(x)
    })

    override fun sqrt() = HalfVectorValue(FloatArray(this.data.size) {
        kotlin.math.sqrt(this.data[it])
    })

    override fun abs() = HalfVectorValue(FloatArray(this.data.size) {
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

    override fun dot(other: VectorValue<*>): FloatValue = when (other) {
        is HalfVectorValue -> {
            var sum = 0.0f
            for (i in this.data.indices) {
                sum = Math.fma(this.data[i], other.data[i], sum)
            }
            FloatValue(sum)
        }
        else -> {
            var sum = 0.0f
            for (i in this.data.indices) {
                sum += Math.fma(this.data[i], other[i].value.toFloat(), sum)
            }
            FloatValue(sum)
        }
    }

    override fun l1(other: VectorValue<*>): DoubleValue = when (other) {
        is HalfVectorValue -> {
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
        is HalfVectorValue -> {
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
        is HalfVectorValue -> {
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
        is HalfVectorValue -> {
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