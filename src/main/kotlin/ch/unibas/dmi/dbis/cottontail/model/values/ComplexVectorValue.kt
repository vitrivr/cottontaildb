package ch.unibas.dmi.dbis.cottontail.model.values

import ch.unibas.dmi.dbis.cottontail.model.values.complex.Complex

/**
 * This is an abstraction over an [Array] and it represents a vector of [Complex]s.
 *
 * @author Manuel Huerbin
 * @version 1.0
 */
inline class ComplexVectorValue(override val value: Array<Complex>) : VectorValue<Array<Complex>> {

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
        get() = this.value.indices

    /**
     * Returns the i-th real entry of this [ComplexVectorValue].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun get(i: Int): Number = this.value[i][0]

    /**
     * Returns the i-th real entry of this [ComplexVectorValue] as [Boolean].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsBool(i: Int): Boolean = this.value[i][0] == 0.0f

    /**
     * Returns true, if this [ComplexVectorValue] consists of all zeroes, i.e. [0, 0, ... 0]
     *
     * @return True, if this [ComplexVectorValue] consists of all zeroes
     */
    override fun allZeros(): Boolean = this.value.all { it[0] == 0.0f && it[1] == 0.0f }

    /**
     * Returns true, if this [ComplexVectorValue] consists of all ones, i.e. [1, 1, ... 1]
     *
     * @return True, if this [ComplexVectorValue] consists of all ones
     */
    override fun allOnes(): Boolean = this.value.all { it[0] == 1.0f && it[1] == 1.0f }

    /**
     * Creates and returns a copy of this [ComplexVectorValue].
     *
     * @return Exact copy of this [ComplexVectorValue].
     */
    override fun copy(): ComplexVectorValue {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun plus(other: VectorValue<Array<Complex>>): VectorValue<Array<Complex>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun minus(other: VectorValue<Array<Complex>>): VectorValue<Array<Complex>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun times(other: VectorValue<Array<Complex>>): VectorValue<Array<Complex>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun div(other: VectorValue<Array<Complex>>): VectorValue<Array<Complex>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun plus(other: Number): VectorValue<Array<Complex>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun minus(other: Number): VectorValue<Array<Complex>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun times(other: Number): VectorValue<Array<Complex>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun div(other: Number): VectorValue<Array<Complex>> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}