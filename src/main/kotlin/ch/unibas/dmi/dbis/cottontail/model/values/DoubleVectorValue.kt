package ch.unibas.dmi.dbis.cottontail.model.values

/**
 * This is an abstraction over a [DoubleArray] and it represents a vector of [Double]s.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
inline class DoubleVectorValue(override val value: DoubleArray) : VectorValue<DoubleArray> {

    constructor(input: List<Number>) : this(DoubleArray(input.size) { input[it].toDouble() })
    constructor(input: Array<Number>) : this(DoubleArray(input.size) { input[it].toDouble() })

    override val size: Int
        get() = this.value.size

    override val numeric: Boolean
        get() = false

    override fun compareTo(other: Value<*>): Int {
        throw IllegalArgumentException("DoubleVectorValues can can only be compared for equality.")
    }

    /**
     * Returns the i-th entry of  this [DoubleVectorValue].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun get(i: Int): Number = this.value[i]

    /**
     * Returns the i-th entry of  this [DoubleVectorValue] as [Double].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsDouble(i: Int): Double = this.value[i]

    /**
     * Returns the i-th entry of  this [DoubleVectorValue] as [Boolean].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsBool(i: Int) = this.value[i] == 0.0

    /**
     * Returns true, if this [DoubleVectorValue] consists of all zeroes, i.e. [0, 0, ... 0]
     *
     * @return True, if this [DoubleVectorValue] consists of all zeroes
     */
    override fun allZeros(): Boolean = this.value.all { it == 0.0 }

    /**
     * Returns true, if this [DoubleVectorValue] consists of all ones, i.e. [1, 1, ... 1]
     *
     * @return True, if this [DoubleVectorValue] consists of all ones
     */
    override fun allOnes(): Boolean = this.value.all { it == 1.0 }

    /**
     * Returns the indices of this [DoubleVectorValue].
     *
     * @return The indices of this [DoubleVectorValue]
     */
    override val indices: IntRange
        get() = this.value.indices
}