package org.vitrivr.cottontail.core.values

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.core.toDouble
import org.vitrivr.cottontail.core.toInt
import org.vitrivr.cottontail.core.types.NumericValue
import org.vitrivr.cottontail.core.types.RealVectorValue
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.core.types.*
import org.vitrivr.cottontail.grpc.CottontailGrpc
import java.util.*
import kotlin.math.pow

/**
 * This is an abstraction over a [BooleanArray] and it represents a vector of [Boolean]s.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
@Serializable
@SerialName("BooleanVector")
@JvmInline
value class BooleanVectorValue(val data: BooleanArray) : RealVectorValue<Int>, PublicValue {
    constructor(input: List<Number>) : this(BooleanArray(input.size) { input[it].toInt() == 1 })
    constructor(input: Array<Number>) : this(BooleanArray(input.size) { input[it].toInt() == 1 })
    constructor(input: Array<Boolean>) : this(BooleanArray(input.size) { input[it] })

    /** The logical size of this [BooleanVectorValue]. */
    override val logicalSize: Int
        get() = this.data.size

    /** The [Types] size of this [BooleanVectorValue]. */
    override val type: Types.Vector<BooleanVectorValue, BooleanValue>
        get() = Types.BooleanVector(this.logicalSize)

    /**
     * Compares this [BooleanVectorValue] to another [BooleanVectorValue]. The comparison is done lexicographically.
     *
     * @param other [Value] to compare to.
     * @return True if equal, false otherwise.
     */
    override fun compareTo(other: Value): Int {
        require(other is BooleanVectorValue) { "BooleanVectorValue can only be compared to another BooleanVectorValue. This is a programmer's error!"}
        return Arrays.compare(this.data, other.data)
    }

    /**
     * Checks for equality between this [BooleanVectorValue] and the other [Value]. Equality can only be
     * established if the other [Value] is also a [BooleanVectorValue] and holds the same value.
     *
     * @param other [Value] to compare to.
     * @return True if equal, false otherwise.
     */
    override fun isEqual(other: Value): Boolean = (other is BooleanVectorValue) && (this.data.contentEquals(other.data))

    /**
     * Converts this [BooleanVectorValue] to a  [CottontailGrpc.Literal] gRCP representation.
     *
     * @return [CottontailGrpc.Literal]
     */
    override fun toGrpc(): CottontailGrpc.Literal
        = CottontailGrpc.Literal.newBuilder().setVectorData(CottontailGrpc.Vector.newBuilder().setBoolVector(CottontailGrpc.BoolVector.newBuilder().addAllVector(this.data.toList()))).build()


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
    override fun get(i: Int): IntValue = IntValue(this.data[i].toInt())

    /**
     * Returns a sub vector of this [BooleanVectorValue] starting at the component [start] and
     * containing [length] components.
     *
     * @param start Index of the first entry of the returned vector.
     * @param length how many elements, including start, to return
     *
     * @return The [BooleanVectorValue] representing the sub-vector.
     */
    override fun slice(start: Int, length: Int) = BooleanVectorValue(this.data.copyOfRange(start, start + length))

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
    override fun copy():BooleanVectorValue = BooleanVectorValue(this.data.copyOf())

    /**
     * Creates and returns a new instance of [BooleanVectorValue] of the same size.
     *
     * @return New instance of [BooleanVectorValue]
     */
    override fun new(): BooleanVectorValue = BooleanVectorValue(BooleanArray(this.data.size))

    override fun plus(other: VectorValue<*>): VectorValue<Int> = when (other) {
        is BooleanVectorValue -> IntVectorValue(
            IntArray(this.data.size) {
                (this.data[it].toInt() + other.data[it].toInt())
            })
        is IntVectorValue -> IntVectorValue(
            IntArray(this.data.size) {
                (this.data[it].toInt() + other.data[it])
            })
        else -> IntVectorValue(IntArray(this.data.size) {
            (this.data[it].toInt() + other[it].asInt().value)
        })
    }

    override fun minus(other: VectorValue<*>): VectorValue<Int> = when (other) {
        is BooleanVectorValue -> IntVectorValue(
            IntArray(this.data.size) {
                (this.data[it].toInt() - other.data[it].toInt())
            })
        is IntVectorValue -> IntVectorValue(
            IntArray(this.data.size) {
                (this.data[it].toInt() - other.data[it])
            })
        else -> IntVectorValue(IntArray(this.data.size) {
            (this.data[it].toInt() - other[it].asInt().value)
        })
    }

    override fun times(other: VectorValue<*>): VectorValue<Int> = when (other) {
        is BooleanVectorValue -> IntVectorValue(
            IntArray(this.data.size) {
                (this.data[it].toInt() * other.data[it].toInt())
            })
        is IntVectorValue -> IntVectorValue(
            IntArray(this.data.size) {
                (this.data[it].toInt() * other.data[it])
            })
        else -> IntVectorValue(IntArray(this.data.size) {
            (this.data[it].toInt() * other[it].asInt().value)
        })
    }

    override fun div(other: VectorValue<*>): VectorValue<Int> = when (other) {
        is BooleanVectorValue -> IntVectorValue(
            IntArray(this.data.size) {
                (this.data[it].toInt() / other.data[it].toInt())
            })
        is IntVectorValue -> IntVectorValue(
            IntArray(this.data.size) {
                (this.data[it].toInt() / other.data[it])
            })
        else -> IntVectorValue(IntArray(this.data.size) {
            (this.data[it].toInt() / other[it].asInt().value)
        })
    }

    override fun plus(other: NumericValue<*>): VectorValue<Int> =
        IntVectorValue(IntArray(this.logicalSize) {
            (this.data[it].toInt() + other.asInt().value)
        })

    override fun minus(other: NumericValue<*>): VectorValue<Int> =
        IntVectorValue(IntArray(this.logicalSize) {
            (this.data[it].toInt() - other.asInt().value)
        })

    override fun times(other: NumericValue<*>): VectorValue<Int> =
        IntVectorValue(IntArray(this.logicalSize) {
            (this.data[it].toInt() * other.asInt().value)
        })

    override fun div(other: NumericValue<*>): VectorValue<Int> =
        IntVectorValue(IntArray(this.logicalSize) {
            (this.data[it].toInt() / other.asInt().value)
        })

    override fun pow(x: Int): DoubleVectorValue =
        DoubleVectorValue(DoubleArray(this.data.size) {
            this.data[it].toDouble().pow(x.toDouble())
        })

    override fun sqrt(): DoubleVectorValue =
        DoubleVectorValue(DoubleArray(this.data.size) {
            if (this.data[it]) {
                1.0
            } else {
                0.0
            }
        })

    override fun abs(): RealVectorValue<Int> = this.copy()

    override fun sum(): DoubleValue = DoubleValue(this.data.sumOf {
        if (it) {
            1.0
        } else {
            0.0
        }
    })

    override fun norm2(): DoubleValue {
        var sum = 0.0
        for (i in this.data) {
            if (i) sum += 1.0
        }
        return DoubleValue(kotlin.math.sqrt(sum))
    }

    override fun dot(other: VectorValue<*>): DoubleValue = when (other) {
        is BooleanVectorValue -> {
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

    override fun hamming(other: VectorValue<*>): IntValue = when (other) {
        is BooleanVectorValue -> {
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