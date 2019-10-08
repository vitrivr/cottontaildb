package ch.unibas.dmi.dbis.cottontail.model.values

import ch.unibas.dmi.dbis.cottontail.model.values.complex.Complex
import ch.unibas.dmi.dbis.cottontail.model.values.complex.ComplexArray

/**
 * This is an abstraction over a [ComplexArray] and it represents a vector of [Complex]s.
 *
 * @author Manuel Huerbin
 * @version 1.0
 */
inline class ComplexVectorValue(override val value: ComplexArray) : VectorValue<ComplexArray> {

    //constructor(input: List<Number>) : this(ComplexArray(input.size) { input[it].toFloat() }) // TODO toComplex()
    //constructor(input: Array<Number>) : this(ComplexArray(input.size) { input[it].toFloat() }) // TODO toComplex()

    override val size: Int
        get() = this.value.size

    override val numeric: Boolean
        get() = false

    override fun compareTo(other: Value<*>): Int {
        throw IllegalArgumentException("ComplexVectorValues can can only be compared for equality.")
    }

    /**
     * Returns the indices of this [ComplexVectorValue].
     *
     * @return The indices of this [ComplexVectorValue]
     */
    override val indices: IntRange
        get() = TODO()

    /**
     * Returns the i-th entry of  this [ComplexVectorValue].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun get(i: Int): Number {
        TODO()
    }

    /**
     * Returns the i-th entry of  this [ComplexVectorValue] as [Boolean].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsBool(i: Int): Boolean {
        TODO()
    }

    /**
     * Returns true, if this [ComplexVectorValue] consists of all zeroes, i.e. [0, 0, ... 0]
     *
     * @return True, if this [ComplexVectorValue] consists of all zeroes
     */
    override fun allZeros(): Boolean {
        TODO()
    }

    /**
     * Returns true, if this [ComplexVectorValue] consists of all ones, i.e. [1, 1, ... 1]
     *
     * @return True, if this [ComplexVectorValue] consists of all ones
     */
    override fun allOnes(): Boolean {
        TODO()
    }

    /**
     * Creates and returns a copy of this [ComplexVectorValue].
     *
     * @return Exact copy of this [ComplexVectorValue].
     */
    override fun copy(): VectorValue<ComplexArray> {
        TODO()
    }

    override fun plus(other: VectorValue<ComplexArray>): VectorValue<ComplexArray> {
        TODO()
    }

    override fun minus(other: VectorValue<ComplexArray>): VectorValue<ComplexArray> {
        TODO()
    }

    override fun times(other: VectorValue<ComplexArray>): VectorValue<ComplexArray> {
        TODO()
    }

    override fun div(other: VectorValue<ComplexArray>): VectorValue<ComplexArray> {
        TODO()
    }

    override fun plus(other: Number): VectorValue<ComplexArray> {
        TODO()
    }

    override fun minus(other: Number): VectorValue<ComplexArray> {
        TODO()
    }

    override fun times(other: Number): VectorValue<ComplexArray> {
        TODO()
    }

    override fun div(other: Number): VectorValue<ComplexArray> {
        TODO()
    }
}