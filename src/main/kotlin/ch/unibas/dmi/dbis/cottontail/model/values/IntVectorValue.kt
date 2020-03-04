package ch.unibas.dmi.dbis.cottontail.model.values

import java.util.*
import kotlin.math.pow

/**
 * This is an abstraction over an [IntArray] and it represents a vector of [Int]s.
 *
 * @author Ralph Gasser
 * @version 1.1
 */
inline class IntVectorValue(override val value: IntArray) : VectorValue<IntArray> {

    constructor(input: List<Number>) : this(IntArray(input.size) { input[it].toInt() })
    constructor(input: Array<Number>) : this(IntArray(input.size) { input[it].toInt() })

    override val size: Int
        get() = this.value.size

    override val numeric: Boolean
        get() = false

    override fun compareTo(other: Value<*>): Int {
        throw IllegalArgumentException("IntVectorValues can can only be compared for equality.")
    }

    /**
     * Returns the indices of this [IntVectorValue].
     *
     * @return The indices of this [IntVectorValue]
     */
    override val indices: IntRange
        get() = this.value.indices

    /**
     * Returns the i-th entry of  this [IntVectorValue].
     *
     * @param i Index of the entry.
     */
    override fun get(i: Int): Number = this.value[i]

    /**
     * Returns the i-th entry of  this [IntVectorValue] as [Int].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsInt(i: Int) = this.value[i]

    /**
     * Returns the i-th entry of  this [IntVectorValue] as [Boolean].
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsBool(i: Int) = this.value[i] != 0

    /**
     * Returns true, if this [IntVectorValue] consists of all zeroes, i.e. [0, 0, ... 0]
     *
     * @return True, if this [IntVectorValue] consists of all zeroes
     */
    override fun allZeros(): Boolean = this.value.all { it == 0 }

    /**
     * Returns true, if this [IntVectorValue] consists of all ones, i.e. [1, 1, ... 1]
     *
     * @return True, if this [IntVectorValue] consists of all ones
     */
    override fun allOnes(): Boolean = this.value.all { it == 1 }

    /**
     * Creates and returns a copy of this [IntVectorValue].
     *
     * @return Exact copy of this [IntVectorValue].
     */
    override fun copy(): IntVectorValue = IntVectorValue(this.value.copyOf(this.size))

    override fun randomInPlace(random: SplittableRandom): IntVectorValue {
        Arrays.setAll(this.value) { random.nextInt() }
        return this
    }

    override fun plusInPlace(other: VectorValue<*>): IntVectorValue {
        Arrays.setAll(this.value) { this.value[it] + other.getAsInt(it) }
        return this
    }

    override fun minusInPlace(other: VectorValue<*>): IntVectorValue {
        Arrays.setAll(this.value) { this.value[it] - other.getAsInt(it) }
        return this
    }

    override fun timesInPlace(other: VectorValue<*>): IntVectorValue {
        Arrays.setAll(this.value) { this.value[it] * other.getAsInt(it) }
        return this
    }

    override fun divInPlace(other: VectorValue<*>): IntVectorValue {
        Arrays.setAll(this.value) { this.value[it] / other.getAsInt(it) }
        return this
    }

    override fun plusInPlace(other: Number): IntVectorValue {
        Arrays.setAll(this.value) { this.value[it] + other.toInt() }
        return this
    }

    override fun minusInPlace(other: Number): IntVectorValue {
        Arrays.setAll(this.value) { this.value[it] - other.toInt() }
        return this
    }

    override fun timesInPlace(other: Number): IntVectorValue {
        Arrays.setAll(this.value) { this.value[it] * other.toInt() }
        return this
    }

    override fun divInPlace(other: Number): IntVectorValue {
        Arrays.setAll(this.value) { this.value[it] / other.toInt() }
        return this
    }

    override fun powInPlace(x: Int): IntVectorValue {
        Arrays.setAll(this.value) { this.value[it].toDouble().pow(x).toInt() }
        return this
    }

    override fun sqrtInPlace(): VectorValue<IntArray> {
        Arrays.setAll(this.value) { kotlin.math.sqrt(this.value[it].toDouble()).toInt() }
        return this
    }

    override fun absInPlace(): VectorValue<IntArray> {
        Arrays.setAll(this.value) { kotlin.math.abs(this.value[it]) }
        return this
    }

    override fun componentsEqual(other: VectorValue<*>): IntVectorValue = IntVectorValue(IntArray(this.value.size) { if (this.value[it] == other.getAsInt(it)) { 1 } else { 0 } })

    override fun sum(): Double = this.value.sum().toDouble()
}