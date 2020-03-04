package ch.unibas.dmi.dbis.cottontail.model.values

import java.util.*
import kotlin.math.pow

/**
 * This is an abstraction over a [DoubleArray] and it represents a vector of [Double]s.
 *
 * @author Ralph Gasser
 * @version 1.1
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
     * Returns the indices of this [DoubleVectorValue].
     *
     * @return The indices of this [DoubleVectorValue]
     */
    override val indices: IntRange
        get() = this.value.indices

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
    override fun getAsBool(i: Int) = this.value[i] != 0.0

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
     * Creates and returns a copy of this [DoubleVectorValue].
     *
     * @return Exact copy of this [DoubleVectorValue].
     */
    override fun copy(): DoubleVectorValue = DoubleVectorValue(this.value.copyOf(this.size))

    override fun randomInPlace(random: SplittableRandom): DoubleVectorValue {
        Arrays.setAll(this.value) { Double.fromBits(random.nextLong()) }
        return this
    }

    override fun plusInPlace(other: VectorValue<*>): DoubleVectorValue {
        Arrays.setAll(this.value) { this.value[it] + other.getAsDouble(it) }
        return this
    }
    override fun minusInPlace(other: VectorValue<*>): DoubleVectorValue {
        Arrays.setAll(this.value) { this.value[it] - other.getAsDouble(it) }
        return this
    }
    override fun timesInPlace(other: VectorValue<*>): DoubleVectorValue {
        Arrays.setAll(this.value) { this.value[it] * other.getAsDouble(it) }
        return this
    }
    override fun divInPlace(other: VectorValue<*>): DoubleVectorValue {
        Arrays.setAll(this.value) { this.value[it] / other.getAsDouble(it) }
        return this
    }

    override fun plusInPlace(other: Number): DoubleVectorValue {
        Arrays.setAll(this.value) { this.value[it] + other.toDouble() }
        return this
    }

    override fun minusInPlace(other: Number): DoubleVectorValue {
        Arrays.setAll(this.value) { this.value[it] - other.toDouble() }
        return this
    }

    override fun timesInPlace(other: Number): DoubleVectorValue {
        Arrays.setAll(this.value) { this.value[it] * other.toDouble() }
        return this
    }

    override fun divInPlace(other: Number): DoubleVectorValue {
        Arrays.setAll(this.value) { this.value[it] / other.toDouble() }
        return this
    }

    override fun powInPlace(x: Int): DoubleVectorValue {
        Arrays.setAll(this.value) { this.value[it].pow(x) }
        return this
    }

    override fun sqrtInPlace(): DoubleVectorValue {
        Arrays.setAll(this.value) { kotlin.math.sqrt(this.value[it]) }
        return this
    }

    override fun absInPlace(): DoubleVectorValue {
        Arrays.setAll(this.value) { kotlin.math.abs(this.value[it]) }
        return this
    }

    override fun componentsEqual(other: VectorValue<*>): VectorValue<DoubleArray> = DoubleVectorValue(DoubleArray(this.value.size) { if (this.value[it] == other.getAsDouble(it)) { 1.0 } else { 0.0 } })

    override fun sum(): Double = this.value.sum()

}