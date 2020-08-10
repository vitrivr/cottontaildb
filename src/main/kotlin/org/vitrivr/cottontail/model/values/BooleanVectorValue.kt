package org.vitrivr.cottontail.model.values

import org.vitrivr.cottontail.model.values.types.*
import org.vitrivr.cottontail.utilities.extensions.toInt
import java.util.*

/**
 * This is an abstraction over a [BooleanArray] and it represents a vector of [Boolean]s.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
inline class BooleanVectorValue(val data: BooleanArray) : VectorValue<Int> {

    companion object {
        /**
         * Generates a [IntVectorValue] of the given size initialized with random numbers.
         *
         * @param size Size of the new [IntVectorValue]
         * @param rnd A [SplittableRandom] to generate the random numbers.
         */
        fun random(size: Int, rnd: SplittableRandom = SplittableRandom(System.currentTimeMillis())) = BooleanVectorValue(BooleanArray(size) { rnd.nextBoolean() })

        /**
         * Generates a [IntVectorValue] of the given size initialized with ones.
         *
         * @param size Size of the new [IntVectorValue]
         */
        fun one(size: Int) = BooleanVectorValue(BooleanArray(size) { true })

        /**
         * Generates a [IntVectorValue] of the given size initialized with zeros.
         *
         * @param size Size of the new [IntVectorValue]
         */
        fun zero(size: Int) = BooleanVectorValue(BooleanArray(size))
    }

    constructor(input: List<Number>) : this(BooleanArray(input.size) { input[it].toInt() == 1 })
    constructor(input: Array<Number>) : this(BooleanArray(input.size) { input[it].toInt() == 1 })
    constructor(input: Array<Boolean>) : this(BooleanArray(input.size) { input[it] })

    override val logicalSize: Int
        get() = data.size

    override fun compareTo(other: Value): Int {
        TODO("Not yet implemented")
    }


    /**
     * Returns the indices of this [BooleanVectorValue].
     *
     * @return The indices of this [BooleanVectorValue]
     */
    override val indices: IntRange
        get() = this.data.indices

    /**
     * Returns the i-th entry of  this [BooleanVectorValue].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun get(i: Int): IntValue = IntValue(this.data[i].toInt())

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
    override fun copy(): BooleanVectorValue = BooleanVectorValue(this.data.copyOf())

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

    override fun plus(other: NumericValue<*>): VectorValue<Int>  {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun minus(other: NumericValue<*>): VectorValue<Int>  {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun times(other: NumericValue<*>): VectorValue<Int>  {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun div(other: NumericValue<*>): VectorValue<Int>  {
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

    override fun sum(): IntValue {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun norm2(): RealValue<*> {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }

    override fun dot(other: VectorValue<*>): RealValue<*> {
        throw UnsupportedOperationException("A BooleanVector array cannot be used to perform arithmetic operations!")
    }
}