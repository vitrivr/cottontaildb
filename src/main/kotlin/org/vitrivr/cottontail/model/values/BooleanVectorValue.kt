package org.vitrivr.cottontail.model.values

import org.vitrivr.cottontail.model.values.types.*
import org.vitrivr.cottontail.utilities.extensions.init
import org.vitrivr.cottontail.utilities.extensions.toInt
import java.util.*

/**
 * This is an abstraction over a [BitSet] and it represents a vector of [Boolean]s.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
inline class BooleanVectorValue(val value: BooleanArray) : VectorValue<Int> {


    constructor(input: List<Number>) : this(BooleanArray(input.size) { input[it].toInt() == 1 })
    constructor(input: Array<Number>) : this(BooleanArray(input.size) { input[it].toInt() == 1 })
    constructor(input: Array<Boolean>) : this(BooleanArray(input.size) { input[it] })

    override val logicalSize: Int
        get() = this.value.size

    override fun compareTo(other: Value): Int {
        TODO("Not yet implemented")
    }


    /**
     * Returns the indices of this [BooleanVectorValue].
     *
     * @return The indices of this [BooleanVectorValue]
     */
    override val indices: IntRange
        get() = this.value.indices

    /**
     * Returns the i-th entry of  this [BooleanVectorValue].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun get(i: Int): IntValue = IntValue(this.value[i].toInt())

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
    override fun copy(): BooleanVectorValue = BooleanVectorValue(BitSet(this.logicalSize).init { this.value[it] })

    override fun plus(other: VectorValue<*>): VectorValue<Int> {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun minus(other: VectorValue<*>): VectorValue<Int> {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun times(other: VectorValue<*>): VectorValue<Int> {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun div(other: VectorValue<*>): VectorValue<Int> {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun plus(other: NumericValue<*>): BooleanVectorValue {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun minus(other: NumericValue<*>): BooleanVectorValue {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun times(other: NumericValue<*>): BooleanVectorValue {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun div(other: NumericValue<*>): BooleanVectorValue {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun pow(x: Int): DoubleVectorValue {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun sqrt(): DoubleVectorValue {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun abs(): RealVectorValue<Int> {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun sum(): ByteValue {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun norm2(): RealValue<*> {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun dot(other: VectorValue<*>): RealValue<*> {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }
}