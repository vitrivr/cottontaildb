package org.vitrivr.cottontail.core.values

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.core.types.*
import org.vitrivr.cottontail.core.types.*
import org.vitrivr.cottontail.grpc.CottontailGrpc
import java.util.*
import kotlin.math.absoluteValue
import kotlin.math.pow

/**
 * This is an abstraction over a [DoubleArray] and it represents a vector of [Double]s.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
@Serializable
@SerialName("DoubleVector")
@JvmInline
value class DoubleVectorValue(val data: DoubleArray) : RealVectorValue<Double>, PublicValue {
    constructor(input: List<Number>) : this(DoubleArray(input.size) { input[it].toDouble() })
    constructor(input: Array<Number>) : this(DoubleArray(input.size) { input[it].toDouble() })
    constructor(input: FloatArray) : this(DoubleArray(input.size) { input[it].toDouble() })
    constructor(input: LongArray) : this(DoubleArray(input.size) { input[it].toDouble() })
    constructor(input: IntArray) : this(DoubleArray(input.size) { input[it].toDouble() })

    /** The logical size of this [DoubleVectorValue]. */
    override val logicalSize: Int
        get() = this.data.size

    /** The [Types] of this [DoubleVectorValue]. */
    override val type: Types<*>
        get() = Types.DoubleVector(this.logicalSize)

    /**
     * Compares this [DoubleVectorValue] to another [DoubleVectorValue]. The comparison is done lexicographically.
     *
     * @param other [Value] to compare to.
     * @return True if equal, false otherwise.
     */
    override fun compareTo(other: Value): Int {
        require(other is DoubleVectorValue) { "DoubleVectorValue can only be compared to another DoubleVectorValue. This is a programmer's error!"}
        return Arrays.compare(this.data, other.data)
    }

    /**
     * Checks for equality between this [DoubleVectorValue] and the other [Value]. Equality can only be
     * established if the other [Value] is also a [DoubleVectorValue] and holds the same value.
     *
     * @param other [Value] to compare to.
     * @return True if equal, false otherwise.
     */
    override fun isEqual(other: Value): Boolean = (other is DoubleVectorValue) && (this.data.contentEquals(other.data))

    /**
     * Converts this [FloatVectorValue] to a [CottontailGrpc.Literal] gRCP representation.
     *
     * @return [CottontailGrpc.Literal]
     */
    override fun toGrpc(): CottontailGrpc.Literal
        = CottontailGrpc.Literal.newBuilder().setVectorData(CottontailGrpc.Vector.newBuilder().setDoubleVector(CottontailGrpc.DoubleVector.newBuilder().addAllVector(this.map { it.value }))).build()

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
     * Returns a sub vector of this [DoubleVectorValue] starting at the component [start] and
     * containing [length] components.
     *
     * @param start Index of the first entry of the returned vector.
     * @param length how many elements, including start, to return
     *
     * @return The [DoubleVectorValue] representing the sub-vector.
     */
    override fun slice(start: Int, length: Int) = DoubleVectorValue(this.data.copyOfRange(start, start + length))

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

    /**
     * Creates and returns a new instance of [DoubleVectorValue] of the same size.
     *
     * @return New instance of [DoubleVectorValue]
     */
    override fun new(): DoubleVectorValue = DoubleVectorValue(DoubleArray(this.data.size))

    override fun plus(other: VectorValue<*>) = when (other) {
        is DoubleVectorValue -> DoubleVectorValue(DoubleArray(this.data.size) {
            (this.data[it] + other.data[it])
        })
        else -> DoubleVectorValue(DoubleArray(this.data.size) {
            (this.data[it] + other[it].asDouble().value)
        })
    }

    override operator fun minus(other: VectorValue<*>) = when (other) {
        is FloatVectorValue -> DoubleVectorValue(DoubleArray(other.logicalSize) {
            (this.data[it] - other.data[it])
        })
        else -> DoubleVectorValue(DoubleArray(other.logicalSize) {
            (this.data[it] - other[it].value.toDouble())
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

    override infix fun dot(other: VectorValue<*>) = when (other) {
        is DoubleVectorValue -> {
            var sum = 0.0
            for (i in this.data.indices) {
                sum = Math.fma(this.data[i], other.data[i], sum)
            }
            DoubleValue(sum)
        }
        else -> {
            var sum = 0.0
            for (i in this.data.indices) {
                sum = Math.fma(this.data[i], other[i].value.toDouble(), sum)
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