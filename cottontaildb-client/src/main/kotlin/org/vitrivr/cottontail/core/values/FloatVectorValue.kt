package org.vitrivr.cottontail.core.values

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.core.types.NumericValue
import org.vitrivr.cottontail.core.types.RealVectorValue
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.core.types.*
import org.vitrivr.cottontail.grpc.CottontailGrpc
import java.util.*
import kotlin.math.absoluteValue
import kotlin.math.pow

/**
 * This is an abstraction over a [FloatArray] and it represents a vector of [Float]s.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
@Serializable
@SerialName("FloatVector")
@JvmInline
value class FloatVectorValue(val data: FloatArray) : RealVectorValue<Float>, PublicValue {
    constructor(input: List<Number>) : this(FloatArray(input.size) { input[it].toFloat() })
    constructor(input: Array<Number>) : this(FloatArray(input.size) { input[it].toFloat() })
    constructor(input: DoubleArray) : this(FloatArray(input.size) { input[it].toFloat() })
    constructor(input: LongArray) : this(FloatArray(input.size) { input[it].toFloat() })
    constructor(input: IntArray) : this(FloatArray(input.size) { input[it].toFloat() })


    /** The logical size of this [FloatVectorValue]. */
    override val logicalSize: Int
        get() = this.data.size

    /** The [Types] of this [FloatVectorValue]. */
    override val type: Types<*>
        get() = Types.FloatVector(this.logicalSize)

    /**
     * Compares this [FloatVectorValue] to another [FloatVectorValue]. The comparison is done lexicographically.
     *
     * @param other [Value] to compare to.
     * @return True if equal, false otherwise.
     */
    override fun compareTo(other: Value): Int {
        require(other is FloatVectorValue) { "FloatVectorValue can only be compared to another FloatVectorValue. This is a programmer's error!"}
        return Arrays.compare(this.data, other.data)
    }

    /**
     * Checks for equality between this [FloatVectorValue] and the other [Value]. Equality can only be
     * established if the other [Value] is also a [FloatVectorValue] and holds the same value.
     *
     * @param other [Value] to compare to.
     * @return True if equal, false otherwise.
     */
    override fun isEqual(other: Value): Boolean = (other is FloatVectorValue) && (this.data.contentEquals(other.data))

    /**
     * Converts this [FloatVectorValue] to a [CottontailGrpc.Literal] gRCP representation.
     *
     * @return [CottontailGrpc.Literal]
     */
    override fun toGrpc(): CottontailGrpc.Literal
        = CottontailGrpc.Literal.newBuilder().setVectorData(CottontailGrpc.Vector.newBuilder().setFloatVector(CottontailGrpc.FloatVector.newBuilder().addAllVector(this.map { it.value }))).build()

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
     * Returns a sub vector of this [FloatVectorValue] starting at the component [start] and
     * containing [length] components.
     *
     * @param start Index of the first entry of the returned vector.
     * @param length how many elements, including start, to return
     *
     * @return The [FloatVectorValue] representing the sub-vector.
     */
    override fun slice(start: Int, length: Int) = FloatVectorValue(this.data.copyOfRange(start, start + length))

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

    /**
     * Creates and returns a new instance of [FloatVectorValue] of the same size.
     *
     * @return New instance of [FloatVectorValue]
     */
    override fun new(): FloatVectorValue = FloatVectorValue(FloatArray(this.data.size))

    override fun plus(other: VectorValue<*>) = when (other) {
        is FloatVectorValue -> FloatVectorValue(FloatArray(this.data.size) {
            (this.data[it] + other.data[it])
        })
        else -> FloatVectorValue(FloatArray(this.data.size) {
            (this.data[it] + other[it].asFloat().value)
        })
    }

    override operator fun minus(other: VectorValue<*>) = when (other) {
        is FloatVectorValue -> FloatVectorValue(FloatArray(this.data.size) {
            (this.data[it] - other.data[it])
        })
        else -> FloatVectorValue(FloatArray(this.data.size) {
            (this.data[it] - other[it].asFloat().value)
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

    override fun dot(other: VectorValue<*>): FloatValue = when (other) {
        is FloatVectorValue -> {
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