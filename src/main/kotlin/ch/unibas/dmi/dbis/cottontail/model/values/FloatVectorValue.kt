package ch.unibas.dmi.dbis.cottontail.model.values

import java.util.*
import kotlin.math.pow

/**
 * This is an abstraction over a [FloatArray] and it represents a vector of [Float]s.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
inline class FloatVectorValue(override val value: FloatArray) : VectorValue<FloatArray> {

    constructor(input: List<Number>) : this(FloatArray(input.size) { input[it].toFloat() })
    constructor(input: Array<Number>) : this(FloatArray(input.size) { input[it].toFloat() })

    override val size: Int
        get() = this.value.size

    override val numeric: Boolean
        get() = false

    override fun compareTo(other: Value<*>): Int {
        throw IllegalArgumentException("FloatVectorValues can can only be compared for equality.")
    }

    /**
     * Returns the indices of this [FloatVectorValue].
     *
     * @return The indices of this [FloatVectorValue]
     */
    override val indices: IntRange
        get() = this.value.indices

    /**
     * Returns the i-th entry of  this [FloatVectorValue].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun get(i: Int): Number = this.value[i]

    /**
     * Returns the i-th entry of  this [FloatVectorValue] as [Float].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsFloat(i: Int) = this.value[i]

    /**
     * Returns the i-th entry of  this [FloatVectorValue] as [Boolean].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsBool(i: Int) = this.value[i] != 0.0f

    /**
     * Returns true, if this [FloatVectorValue] consists of all zeroes, i.e. [0, 0, ... 0]
     *
     * @return True, if this [FloatVectorValue] consists of all zeroes
     */
    override fun allZeros(): Boolean = this.value.all { it == 0.0f }

    /**
     * Returns true, if this [FloatVectorValue] consists of all ones, i.e. [1, 1, ... 1]
     *
     * @return True, if this [FloatVectorValue] consists of all ones
     */
    override fun allOnes(): Boolean = this.value.all { it == 1.0f }

    /**
     * Creates and returns a copy of this [FloatVectorValue].
     *
     * @return Exact copy of this [FloatVectorValue].
     */
    override fun copy(): FloatVectorValue = FloatVectorValue(this.value.copyOf(this.size))

    override fun randomInPlace(random: SplittableRandom): FloatVectorValue {
        this.value.indices.forEach { this.value[it] = Float.fromBits(random.nextInt()) }
        return this
    }

    override fun plusInPlace(other: VectorValue<*>): FloatVectorValue {
        this.value.indices.forEach {this.value[it] += other.getAsFloat(it) }
        return this
    }

    override fun minusInPlace(other: VectorValue<*>): FloatVectorValue {
        this.value.indices.forEach {this.value[it] -= other.getAsFloat(it) }
        return this
    }

    override fun timesInPlace(other: VectorValue<*>): FloatVectorValue {
        this.value.indices.forEach {this.value[it] *= other.getAsFloat(it) }
        return this
    }

    override fun divInPlace(other: VectorValue<*>): FloatVectorValue {
        this.value.indices.forEach {this.value[it] /= other.getAsFloat(it) }
        return this
    }

    override fun plusInPlace(other: Number): FloatVectorValue {
        this.value.indices.forEach {this.value[it] += other.toFloat() }
        return this
    }

    override fun minusInPlace(other: Number): FloatVectorValue {
        this.value.indices.forEach {this.value[it] -= other.toFloat() }
        return this
    }

    override fun timesInPlace(other: Number): FloatVectorValue {
        this.value.indices.forEach {this.value[it] *= other.toFloat() }
        return this
    }

    override fun divInPlace(other: Number): FloatVectorValue {
        this.value.indices.forEach {this.value[it] /= other.toFloat() }
        return this
    }

    override fun powInPlace(x: Int): FloatVectorValue {
        this.value.indices.forEach {this.value[it] = this.value[it].pow(x) }
        return this
    }

    override fun sqrtInPlace(): VectorValue<FloatArray> {
        this.value.indices.forEach {this.value[it] =  kotlin.math.sqrt(this.value[it]) }
        return this
    }

    override fun absInPlace(): VectorValue<FloatArray> {
        this.value.indices.forEach {this.value[it] = kotlin.math.abs(this.value[it]) }
        return this
    }

    override fun componentsEqual(other: VectorValue<*>): FloatVectorValue = FloatVectorValue(FloatArray(this.value.size) { if (this.value[it] == other.getAsFloat(it)) { 1.0f } else { 0.0f } })

    override fun sum(): Double = this.value.sum().toDouble()
}