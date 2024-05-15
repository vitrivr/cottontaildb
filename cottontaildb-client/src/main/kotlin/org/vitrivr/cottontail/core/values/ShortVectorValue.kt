package org.vitrivr.cottontail.core.values

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.core.types.*
import org.vitrivr.cottontail.grpc.CottontailGrpc
import java.nio.ByteBuffer
import java.nio.ShortBuffer
import java.util.*
import kotlin.math.pow

/**
 * This is an abstraction over a [ShortArray] and it represents a vector of [Short]s.
 *
 */
@Serializable
@SerialName("ShortVector")
@JvmInline
value class ShortVectorValue(val data: ShortArray) : RealVectorValue<Short>, PublicValue {


    constructor(input: List<Number>) : this(ShortArray(input.size) { input[it].toShort() })
    constructor(input: Array<Number>) : this(ShortArray(input.size) { input[it].toShort() })
    constructor(input: DoubleArray) : this(ShortArray(input.size) { input[it].toInt().toShort() })
    constructor(input: FloatArray) : this(ShortArray(input.size) { input[it].toInt().toShort() })
    constructor(input: IntArray) : this(ShortArray(input.size) { input[it].toShort() })
    constructor(input: ShortBuffer) : this(ShortArray(input.remaining()) { input[it] })
    constructor(input: ByteBuffer) : this(input.asShortBuffer())

    /** The logical size of this [ShortVectorValue]. */
    override val logicalSize: Int
        get() = this.data.size

    /** The [Types] of this [ShortVectorValue]. */
    override val type: Types.Vector<ShortVectorValue, ShortValue>
        get() = Types.ShortVector(this.logicalSize)

    /**
     * Compares this [ShortVectorValue] to another [ShortVectorValue]. The comparison is done lexicographically.
     *
     * @param other [Value] to compare to.
     * @return True if equal, false otherwise.
     */
    override fun compareTo(other: Value): Int {
        require(other is ShortVectorValue) { "ShortVectorValue can only be compared to another ShortVectorValue. This is a programmer's error!"}
        return Arrays.compare(this.data, other.data)
    }

    /**
     * Checks for equality between this [ShortVectorValue] and the other [Value]. Equality can only be
     * established if the other [Value] is also a [ShortVectorValue] and holds the same value.
     *
     * @param other [Value] to compare to.
     * @return True if equal, false otherwise.
     */
    override fun isEqual(other: Value): Boolean = (other is ShortVectorValue) && (this.data.contentEquals(other.data))

    /**
     * Converts this [ShortVectorValue] to a [CottontailGrpc.Literal] gRCP representation.
     *
     * @return [CottontailGrpc.Literal]
     */
    override fun toGrpc(): CottontailGrpc.Literal.Builder
        = CottontailGrpc.Literal.newBuilder().setVectorData(CottontailGrpc.Vector.newBuilder().setShort(CottontailGrpc.IntVector.newBuilder().addAllVector(this.map { it.value.toInt() })))

    /**
     * Returns the indices of this [ShortVectorValue].
     *
     * @return The indices of this [ShortVectorValue]
     */
    override val indices: IntRange
        get() = this.data.indices

    /**
     * Returns the i-th entry of  this [ShortVectorValue].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun get(i: Int) = ShortValue(this.data[i])

    /**
     * Returns the i-th entry of  this [ShortVectorValue] as [Boolean].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsBool(i: Int) = this.data[i] != (0).toShort()

    /**
     * Returns true, if this [ShortVectorValue] consists of all zeroes, i.e. [0, 0, ... 0]
     *
     * @return True, if this [ShortVectorValue] consists of all zeroes
     */
    override fun allZeros(): Boolean = this.data.all { it == (0).toShort() }

    /**
     * Returns true, if this [ShortVectorValue] consists of all ones, i.e. [1, 1, ... 1]
     *
     * @return True, if this [ShortVectorValue] consists of all ones
     */
    override fun allOnes(): Boolean = this.data.all { it == (1).toShort() }

    /**
     * Creates and returns a copy of this [ShortVectorValue].
     *
     * @return Exact copy of this [ShortVectorValue].
     */
    override fun copy(): ShortVectorValue = ShortVectorValue(this.data.copyOf(this.logicalSize))

    /**
     * Creates and returns a new instance of [ShortVectorValue] of the same size.
     *
     * @return New instance of [ShortVectorValue]
     */
    override fun new(): ShortVectorValue = ShortVectorValue(ShortArray(this.data.size))

    override fun plus(other: VectorValue<*>): ShortVectorValue = when (other) {
        is ShortVectorValue -> ShortVectorValue(ShortArray(this.data.size) {
            (this.data[it] + other.data[it]).toShort()
        })
        else -> ShortVectorValue(ShortArray(this.data.size) {
            (this.data[it] + other[it].asLong().value).toShort()
        })
    }

    override fun minus(other: VectorValue<*>): ShortVectorValue = when (other) {
        is ShortVectorValue -> ShortVectorValue(ShortArray(this.data.size) {
            (this.data[it] - other.data[it]).toShort()
        })
        else -> ShortVectorValue(ShortArray(this.data.size) {
            (this.data[it] - other[it].asLong().value).toShort()
        })
    }

    override fun times(other: VectorValue<*>): ShortVectorValue = when (other) {
        is ShortVectorValue -> ShortVectorValue(ShortArray(this.data.size) {
            (this.data[it] * other.data[it]).toShort()
        })
        else -> ShortVectorValue(ShortArray(this.data.size) {
            (this.data[it] * other[it].asLong().value).toShort()
        })
    }

    override fun div(other: VectorValue<*>): ShortVectorValue = when (other) {
        is ShortVectorValue -> ShortVectorValue(ShortArray(this.data.size) {
            (this.data[it] / other.data[it]).toShort()
        })
        else -> ShortVectorValue(ShortArray(this.data.size) {
            (this.data[it] / other[it].asLong().value).toShort()
        })
    }

    override fun plus(other: NumericValue<*>): ShortVectorValue {
        val otherAsLong = other.asLong().value
        return ShortVectorValue(ShortArray(this.logicalSize) {
            (this.data[it] + otherAsLong).toShort()
        })
    }

    override fun minus(other: NumericValue<*>): ShortVectorValue {
        val otherAsLong = other.asLong().value
        return ShortVectorValue(ShortArray(this.logicalSize) {
            (this.data[it] - otherAsLong).toShort()
        })
    }

    override fun times(other: NumericValue<*>): ShortVectorValue {
        val otherAsLong = other.asLong().value
        return ShortVectorValue(ShortArray(this.logicalSize) {
            (this.data[it] * otherAsLong).toShort()
        })
    }

    override fun div(other: NumericValue<*>): ShortVectorValue {
        val otherAsLong = other.asLong().value
        return ShortVectorValue(ShortArray(this.logicalSize) {
            (this.data[it] / otherAsLong).toShort()
        })
    }

    override fun pow(x: Int) = DoubleVectorValue(DoubleArray(this.data.size) {
        this.data[it].toDouble().pow(x)
    })

    override fun sqrt() = DoubleVectorValue(DoubleArray(this.data.size) {
        kotlin.math.sqrt(this.data[it].toDouble())
    })

    override fun abs() = ShortVectorValue(ShortArray(this.logicalSize) {
        kotlin.math.abs(this.data[it].toInt()).toShort()
    })

    override fun sum(): DoubleValue = DoubleValue(this.data.map { it.toDouble() }.sum())

    override fun norm2(): DoubleValue {
        var sum = 0.0
        for (i in this.indices) {
            sum += this[i].value * this[i].value
        }
        return DoubleValue(kotlin.math.sqrt(sum))
    }

    override fun dot(other: VectorValue<*>): DoubleValue {
        var sum = 0.0
        for (i in this.indices) {
            sum += other[i].value.toInt() * this[i].value
        }
        return DoubleValue(sum)
    }

    override fun l1(other: VectorValue<*>): DoubleValue = when (other) {
        is ShortVectorValue -> {
            var sum = 0.0
            for (i in this.data.indices) {
                sum += kotlin.math.abs(this.data[i] - other.data[i])
            }
            DoubleValue(sum)
        }
        else -> {
            var sum = 0.0
            for (i in this.data.indices) {
                sum += kotlin.math.abs(this.data[i] - other[i].value.toLong())
            }
            DoubleValue(sum)
        }
    }

    override fun l2(other: VectorValue<*>): DoubleValue = when (other) {
        is ShortVectorValue -> {
            var sum = 0.0
            for (i in this.data.indices) {
                sum += (this.data[i] - other.data[i]).toDouble().pow(2)
            }
            DoubleValue(kotlin.math.sqrt(sum))
        }
        else -> {
            var sum = 0.0
            for (i in this.data.indices) {
                sum += (this.data[i] - other[i].value.toLong()).toDouble().pow(2)
            }
            DoubleValue(kotlin.math.sqrt(sum))
        }
    }

    override fun lp(other: VectorValue<*>, p: Int): DoubleValue = when (other) {
        is ShortVectorValue -> {
            var sum = 0.0
            for (i in this.data.indices) {
                sum += (this.data[i] - other.data[i]) * (this.data[i] - other.data[i]).toDouble().pow(p)
            }
            DoubleValue(sum.pow(1.0 / p))
        }
        else -> {
            var sum = 0.0
            for (i in this.data.indices) {
                sum += (this.data[i] - other[i].value.toInt()).toFloat().pow(p)
            }
            DoubleValue(sum.pow(1.0 / p))
        }
    }

    override fun hamming(other: VectorValue<*>): LongValue = when (other) {
        is ShortVectorValue -> {
            var sum = 0L
            val start = Arrays.mismatch(this.data, other.data)
            for (i in start until other.data.size) {
                if (this.data[i] != other.data[i]) {
                    sum += 1L
                }
            }
            LongValue(sum)
        }
        else -> LongValue(this.data.size)
    }

    /**
     * Returns a sub vector of this [ShortVectorValue] starting at the component [start] and
     * containing [length] components.
     *
     * @param start Index of the first entry of the returned vector.
     * @param length how many elements, including start, to return
     *
     * @return The [ShortVectorValue] representing the sub-vector.
     */
    override fun slice(start: Int, length: Int) = ShortVectorValue(this.data.copyOfRange(start, start + length))
}