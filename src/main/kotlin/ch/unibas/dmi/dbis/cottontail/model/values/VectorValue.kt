package ch.unibas.dmi.dbis.cottontail.model.values

/**
 * This is an abstraction over the existing primitive array types provided by Kotlin. It allows for the advanced type
 * system implemented by Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
interface VectorValue<T> : Value<T> {
    /**
     * Returns the i-th entry of  this [VectorValue].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    operator fun get(i: Int): Number

    /**
     * Returns the i-th entry of  this [VectorValue] as [Double].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    fun getAsDouble(i: Int) = get(i).toDouble()

    /**
     * Returns the i-th entry of  this [VectorValue] as [Float].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    fun getAsFloat(i: Int) = get(i).toFloat()

    /**
     * Returns the i-th entry of  this [VectorValue] as [Long].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    fun getAsLong(i: Int) = get(i).toLong()

    /**
     * Returns the i-th entry of  this [VectorValue] as [Int].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    fun getAsInt(i: Int) = get(i).toInt()

    /**
     * Returns the i-th entry of  this [VectorValue] as [Boolean].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    fun getAsBool(i: Int): Boolean

    /**
     * Returns true, if this [VectorValue] consists of all zeroes, i.e. [0, 0, ... 0]
     *
     * @return True, if this [VectorValue] consists of all zeroes
     */
    fun allZeros(): Boolean

    /**
     * Returns true, if this [VectorValue] consists of all ones, i.e. [1, 1, ... 1]
     *
     * @return True, if this [VectorValue] consists of all ones
     */
    fun allOnes(): Boolean

    /**
     * Returns the indices of this [VectorValue].
     *
     * @return The indices of this [VectorValue]
     */
    val indices: IntRange

    /**
     * Creates and returns a copy of this [VectorValue].
     *
     * @return Exact copy of this [VectorValue].
     */
    fun copy(): VectorValue<T>

    operator fun plus(other: VectorValue<T>): VectorValue<T>
    operator fun minus(other: VectorValue<T>): VectorValue<T>
    operator fun times(other: VectorValue<T>): VectorValue<T>
    operator fun div(other: VectorValue<T>): VectorValue<T>
    operator fun plus(other: Number): VectorValue<T>
    operator fun minus(other: Number): VectorValue<T>
    operator fun times(other: Number): VectorValue<T>
    operator fun div(other: Number): VectorValue<T>
}