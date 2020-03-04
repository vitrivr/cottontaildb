package ch.unibas.dmi.dbis.cottontail.model.values

import java.util.*
import kotlin.math.pow

/**
 * This is an abstraction over an [Array] and it represents a vector of [Complex32]s.
 *
 * @author Manuel Huerbin & Ralph Gasser
 * @version 1.1
 */
inline class Complex32VectorValue(override val value: FloatArray) : VectorValue<FloatArray> {

    /**
     * Size of the [Complex32VectorValue] im terms of elements is actually half the size of the underlying [FloatArray].
     */
    override val size: Int
        get() = this.value.size/2

    override val numeric: Boolean
        get() = false

    override fun compareTo(other: Value<*>): Int {
        throw IllegalArgumentException("ComplexVectorValues can can only be compared for equality.")
    }

    /**
     * Returns the indices of this [Complex32VectorValue].
     *
     * @return The indices of this [Complex32VectorValue]
     */
    override val indices: IntRange
        get() = this.value.indices

    /**
     * Returns the i-th entry of  this [Complex32VectorValue]. All entries with i % 2 == 0 correspond
     * to the real part of the value, whereas entries with i % 2 == 1 correspond to the imaginary part.
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun get(i: Int): Number = this.value[i]

    /**
     * Returns the i-th entry of this [Complex32VectorValue] as [Float]. All entries with i % 2 == 0 correspond
     * to the real part of the value, whereas entries with i % 2 == 1 correspond to the imaginary part.
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsFloat(i: Int): Float = this.value[i]

    /**
     * Returns the i-th entry of  this [Complex32VectorValue] as [Boolean]. All entries with index % 2 == 0 correspond
     * to the real part of the value, whereas entries with i % 2 == 1 correspond to the imaginary part.
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsBool(i: Int): Boolean = this.value[i] != 0.0f

    /**
     * Returns true, if this [Complex32VectorValue] consists of all zeroes, i.e. [0, 0, ... 0]
     *
     * @return True, if this [Complex32VectorValue] consists of all zeroes
     */
    override fun allZeros(): Boolean = this.value.all { it == 0.0f }

    /**
     * Returns true, if this [Complex32VectorValue] consists of all ones, i.e. [1, 1, ... 1]
     *
     * @return True, if this [Complex32VectorValue] consists of all ones
     */
    override fun allOnes(): Boolean = this.value.all { it == 1.0f }

    /**
     * Creates and returns a copy of this [Complex32VectorValue].
     *
     * @return Exact copy of this [Complex32VectorValue].
     */
    override fun copy(): Complex32VectorValue = Complex32VectorValue(value.copyOf())
    override fun randomInPlace(random: SplittableRandom): Complex32VectorValue {
        this.value.indices.forEach { this.value[it] = Float.fromBits(random.nextInt()) }
        return this
    }

    override fun plusInPlace(other: VectorValue<*>): Complex32VectorValue {
        this.value.indices.forEach {this.value[it] += other.getAsFloat(it) }
        return this
    }

    override fun minusInPlace(other: VectorValue<*>): Complex32VectorValue {
        this.value.indices.forEach {this.value[it] -= other.getAsFloat(it) }
        return this
    }

    override fun timesInPlace(other: VectorValue<*>): Complex32VectorValue {
        (0 until this.size step 2).forEach {
            this.value[it] = this.value[it] * other.getAsFloat(it) - this.value[it+1] * other.getAsFloat(it)
            this.value[it+1] = this.value[it] * other.getAsFloat(it+1) + this.value[it+1] * other.getAsFloat(it)
        }
        return this
    }

    override fun divInPlace(other: VectorValue<*>): Complex32VectorValue {
        (0 until this.size step 2).forEach {
            val div = (this.value[it+1].pow(2) + other.getAsFloat(it+1).pow(2))
            this.value[it] = (this.value[it] * other.getAsFloat(it) + this.value[it+1] * other.getAsFloat(it)) / div
            this.value[it+1] = (this.value[it+1] * other.getAsFloat(it) - this.value[it] * other.getAsFloat(it+1)) / div
        }
        return this
    }

    override fun plusInPlace(other: Number): Complex32VectorValue {
        (0 until this.size step 2).forEach {this.value[it] += other.toFloat() }
        return this
    }

    override fun minusInPlace(other: Number): Complex32VectorValue {
        (0 until this.size step 2).forEach {this.value[it] -= other.toFloat() }
        return this
    }

    override fun timesInPlace(other: Number): Complex32VectorValue {
        (0 until this.size step 2).forEach {this.value[it] *= other.toFloat() }
        return this
    }

    override fun divInPlace(other: Number): Complex32VectorValue {
        (0 until this.size step 2).forEach {this.value[it] /= other.toFloat() }
        return this
    }
    override fun powInPlace(x: Int): Complex32VectorValue {
        (0 until this.size step 2).forEach {
            val a2 = this.value[it].pow(2)
            val b2 = this.value[it+1].pow(2)
            val r = kotlin.math.sqrt(a2 + b2)
            val theta = kotlin.math.atan(b2 / a2)
            this.value[it] = r.pow(x) * kotlin.math.cos(theta * x)
            this.value[it+1] = r.pow(x) * kotlin.math.sin(theta * x)
        }
        return this
    }

    override fun sqrtInPlace(): Complex32VectorValue {
        (0 until this.size step 2).forEach {
            this.value[it] = kotlin.math.sqrt((this.value[it] + kotlin.math.sqrt(this.value[it].pow(2) + this.value[it+1].pow(2)))/2)
            this.value[it+1] = (this.value[it+1] / kotlin.math.abs(this.value[it+1])) * kotlin.math.sqrt((- this.value[it] + kotlin.math.sqrt(this.value[it].pow(2) + this.value[it+1].pow(2)))/2)
        }
        return this
    }

    override fun absInPlace(): Complex32VectorValue {
        this.value.indices.forEach {this.value[it] = kotlin.math.abs(this.value[it]) }
        return this
    }

    override fun componentsEqual(other: VectorValue<*>): Complex32VectorValue = Complex32VectorValue(FloatArray(this.value.size) { if (this.value[it] == other.getAsFloat(it)) { 1.0f } else { 0.0f } })

    override fun sum(): Double = (0 until this.size step 2).map { kotlin.math.sqrt(this.value[it].pow(2) + this.value[it].pow(2)) }.sum().toDouble()
}