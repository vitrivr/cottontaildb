package ch.unibas.dmi.dbis.cottontail.model.values

import ch.unibas.dmi.dbis.cottontail.utilities.extensions.*
import java.util.*

/**
 * This is an abstraction over a [BitSet] and it represents a vector of [Boolean]s.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
inline class BooleanVectorValue(override val value: BitSet) : VectorValue<BitSet> {
    constructor(input: List<Number>) : this(BitSet(input.size).init { input[it].toInt() == 1 })
    constructor(input: Array<Number>) : this(BitSet(input.size).init { input[it].toInt() == 1 })
    constructor(input: Array<Boolean>) : this(BitSet(input.size).init { input[it] })

    override val size: Int
        get() = value.length()

    override val numeric: Boolean
        get() = false

    override fun compareTo(other: Value<*>): Int {
        throw IllegalArgumentException("BooleanVectorValues can can only be compared for equality.")
    }
    /**
     * Returns the indices of this [BooleanVectorValue].
     *
     * @return The indices of this [BooleanVectorValue]
     */
    override val indices: IntRange
        get() = IntRange(0, this.value.length()-1)

    /**
     * Returns the i-th entry of  this [BooleanVectorValue].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun get(i: Int): Number = this.value[i].toInt()

    /**
     * Returns the i-th entry of  this [BooleanVectorValue] as [Double].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsDouble(i: Int) = this.value[i].toDouble()

    /**
     * Returns the i-th entry of  this [BooleanVectorValue] as [Float].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsFloat(i: Int) = this.value[i].toFloat()

    /**
     * Returns the i-th entry of  this [BooleanVectorValue] as [Long].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsLong(i: Int) = this.value[i].toLong()

    /**
     * Returns the i-th entry of  this [BooleanVectorValue] as [Int].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsInt(i: Int) = this.value[i].toInt()

    /**
     * Returns the i-th entry of  this [BooleanVectorValue] as [Boolean].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsBool(i: Int) = this.value[i]

    /**
     * Returns true, if this [BooleanVectorValue] consists of all zeroes, i.e. [0, 0, ... 0]
     *
     * @return True, if this [BooleanVectorValue] consists of all zeroes
     */
    override fun allZeros(): Boolean = this.indices.all { !this.value[it] }

    /**
     * Returns true, if this [BooleanVectorValue] consists of all ones, i.e. [1, 1, ... 1]
     *
     * @return True, if this [BooleanVectorValue] consists of all ones
     */
    override fun allOnes(): Boolean = this.indices.all { this.value[it] }

    /**
     * Creates and returns a copy of this [BooleanVectorValue].
     *
     * @return Exact copy of this [BooleanVectorValue].
     */
    override fun copy(): VectorValue<BitSet> = BooleanVectorValue(BitSet(this.size).init { this.value[it] })

    override fun plus(other: VectorValue<*>): VectorValue<BitSet> {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun minus(other: VectorValue<*>): VectorValue<BitSet> {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun times(other: VectorValue<*>): VectorValue<BitSet> {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun div(other: VectorValue<*>): VectorValue<BitSet> {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun plusInPlace(other: VectorValue<*>): VectorValue<BitSet> {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun minusInPlace(other: VectorValue<*>): VectorValue<BitSet> {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun timesInPlace(other: VectorValue<*>): VectorValue<BitSet> {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun divInPlace(other: VectorValue<*>): VectorValue<BitSet> {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun plus(other: Number): VectorValue<BitSet> {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun minus(other: Number): VectorValue<BitSet> {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun times(other: Number): VectorValue<BitSet> {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun div(other: Number): VectorValue<BitSet> {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun pow(x: Int): VectorValue<BitSet> {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun powInPlace(x: Int): VectorValue<BitSet> {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun sqrt(): VectorValue<BitSet> {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun sqrtInPlace(): VectorValue<BitSet> {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun abs(): VectorValue<BitSet> {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun absInPlace(): VectorValue<BitSet> {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun componentsEqual(other: VectorValue<*>): VectorValue<BitSet> {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun sum(): Double {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }
}