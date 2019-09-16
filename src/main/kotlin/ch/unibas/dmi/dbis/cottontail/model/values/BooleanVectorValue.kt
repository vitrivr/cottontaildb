package ch.unibas.dmi.dbis.cottontail.model.values

import ch.unibas.dmi.dbis.cottontail.utilities.extensions.*
import java.util.*

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
     * Returns the i-th entry of  this [IntVectorValue].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun get(i: Int): Number = this.value[i].toInt()

    /**
     * Returns the i-th entry of  this [VectorValue] as [Double].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsDouble(i: Int) = this.value[i].toDouble()

    /**
     * Returns the i-th entry of  this [VectorValue] as [Float].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsFloat(i: Int) = this.value[i].toFloat()

    /**
     * Returns the i-th entry of  this [VectorValue] as [Long].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsLong(i: Int) = this.value[i].toLong()

    /**
     * Returns the i-th entry of  this [VectorValue] as [Int].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsInt(i: Int) = this.value[i].toInt()

    /**
     * Returns the i-th entry of  this [VectorValue] as [Boolean].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsBool(i: Int) = this.value[i]

    /**
     * Returns the indices of this [LongVectorValue].
     *
     * @return The indices of this [LongVectorValue]
     */
    override val indices: IntRange
        get() = IntRange(0, this.value.length()-1)
}