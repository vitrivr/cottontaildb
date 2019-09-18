package ch.unibas.dmi.dbis.cottontail.model.values

/**
 * This is an abstraction over a [FloatArray] and it represents a vector of [Float]s.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
inline class LongVectorValue(override val value: LongArray) : VectorValue<LongArray> {


    constructor(input: List<Number>) : this(LongArray(input.size) { input[it].toLong() })
    constructor(input: Array<Number>) : this(LongArray(input.size) { input[it].toLong() })

    override val size: Int
        get() = value.size

    override val numeric: Boolean
        get() = false

    override fun compareTo(other: Value<*>): Int {
        throw IllegalArgumentException("LongVectorValues can can only be compared for equality.")
    }

    /**
     * Returns the i-th entry of  this [LongVectorValue].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun get(i: Int): Number = this.value[i]

    /**
     * Returns the i-th entry of  this [LongVectorValue] as [Long].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsLong(i: Int) = this.value[i]

    /**
     * Returns the i-th entry of  this [LongVectorValue] as [Boolean].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsBool(i: Int) = this.value[i] == 0L

    /**
     * Returns true, if this [LongVectorValue] consists of all zeroes, i.e. [0, 0, ... 0]
     *
     * @return True, if this [LongVectorValue] consists of all zeroes
     */
    override fun allZeros(): Boolean = this.value.all { it == 0L }

    /**
     * Returns true, if this [LongVectorValue] consists of all ones, i.e. [1, 1, ... 1]
     *
     * @return True, if this [LongVectorValue] consists of all ones
     */
    override fun allOnes(): Boolean = this.value.all { it == 1L }

    /**
     * Returns the indices of this [LongVectorValue].
     *
     * @return The indices of this [LongVectorValue]
     */
    override val indices: IntRange
        get() = this.value.indices
}