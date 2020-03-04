package ch.unibas.dmi.dbis.cottontail.model.values

import java.util.*

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
     * Creates and returns an exact copy of this [VectorValue].
     *
     * @return Exact copy of this [VectorValue].
     */
    fun copy(): VectorValue<T>

    /**
     * Populates this [VectorValue] with random numbers
     *
     * @return This [VectorValue].
     */
    fun random(random: SplittableRandom = SplittableRandom(System.currentTimeMillis())): VectorValue<T> = copy().randomInPlace(random)
    fun randomInPlace(random: SplittableRandom = SplittableRandom(System.currentTimeMillis())): VectorValue<T>

    operator fun plus(other: VectorValue<*>): VectorValue<T> = this.copy().plusInPlace(other)
    operator fun minus(other: VectorValue<*>): VectorValue<T> = this.copy().minusInPlace(other)
    operator fun times(other: VectorValue<*>): VectorValue<T> = this.copy().timesInPlace(other)
    operator fun div(other: VectorValue<*>): VectorValue<T> = this.copy().divInPlace(other)

    fun plusInPlace(other: VectorValue<*>): VectorValue<T>
    fun minusInPlace(other: VectorValue<*>): VectorValue<T>
    fun timesInPlace(other: VectorValue<*>): VectorValue<T>
    fun divInPlace(other: VectorValue<*>): VectorValue<T>

    operator fun plus(other: Number): VectorValue<T> = this.copy().plusInPlace(other)
    operator fun minus(other: Number): VectorValue<T> = this.copy().minusInPlace(other)
    operator fun times(other: Number): VectorValue<T> = this.copy().timesInPlace(other)
    operator fun div(other: Number): VectorValue<T> = this.copy().divInPlace(other)

    fun plusInPlace(other: Number): VectorValue<T>
    fun minusInPlace(other: Number): VectorValue<T>
    fun timesInPlace(other: Number): VectorValue<T>
    fun divInPlace(other: Number): VectorValue<T>

    fun pow (x: Int): VectorValue<T> = this.copy().powInPlace(x)
    fun powInPlace(x: Int): VectorValue<T>

    fun sqrt(): VectorValue<T> = this.copy().sqrtInPlace()
    fun sqrtInPlace():VectorValue<T>

    fun abs(): VectorValue<T> = this.copy().absInPlace()
    fun absInPlace(): VectorValue<T>

    fun componentsEqual(other: VectorValue<*>): VectorValue<T>

    fun sum(): Double

}