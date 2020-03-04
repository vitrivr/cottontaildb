package ch.unibas.dmi.dbis.cottontail.model.values

import java.util.*
import kotlin.math.pow

/**
 * This is an abstraction over an [Array] and it represents a vector of [Complex64]s.
 *
 * @author Manuel Huerbin & Ralph Gasser
 * @version 1.1
 */
inline class Complex64VectorValue(override val value: DoubleArray) : VectorValue<DoubleArray> {

    /**
     * Size of the [Complex64VectorValue] im terms of elements is actually half the size of the underlying [DoubleArray].
     */
    override val size: Int
        get() = this.value.size / 2

    override val numeric: Boolean
        get() = false

    override fun compareTo(other: Value<*>): Int {
        throw IllegalArgumentException("ComplexVectorValues can can only be compared for equality.")
    }

    /**
     * Returns the indices of this [Complex64VectorValue].
     *
     * @return The indices of this [Complex64VectorValue]
     */
    override val indices: IntRange
        get() = this.value.indices

    /**
     * Returns the i-th entry of  this [Complex64VectorValue]. All entries with i % 2 == 0 correspond
     * to the real part of the value, whereas entries with i % 2 == 1 correspond to the imaginary part.
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun get(i: Int): Number = this.value[i]

    /**
     * Returns the i-th entry of this [Complex64VectorValue] as [Double]. All entries with i % 2 == 0 correspond
     * to the real part of the value, whereas entries with i % 2 == 1 correspond to the imaginary part.
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsDouble(i: Int): Double = this.value[i]

    /**
     * Returns the i-th entry of  this [Complex64VectorValue] as [Boolean]. All entries with index % 2 == 0 correspond
     * to the real part of the value, whereas entries with i % 2 == 1 correspond to the imaginary part.
     *
     * @param i Index of the entry.
     * @return The value at index i.
     */
    override fun getAsBool(i: Int): Boolean = this.value[i] != 0.0

    /**
     * Returns true, if this [Complex64VectorValue] consists of all zeroes, i.e. [0, 0, ... 0]
     *
     * @return True, if this [Complex64VectorValue] consists of all zeroes
     */
    override fun allZeros(): Boolean = this.value.all { it == 0.0 }

    /**
     * Returns true, if this [Complex64VectorValue] consists of all ones, i.e. [1, 1, ... 1]
     *
     * @return True, if this [Complex64VectorValue] consists of all ones
     */
    override fun allOnes(): Boolean = this.value.all { it == 1.0 }

    /**
     * Creates and returns a copy of this [Complex64VectorValue].
     *
     * @return Exact copy of this [Complex64VectorValue].
     */
    override fun copy(): Complex64VectorValue = Complex64VectorValue(this.value.copyOf())
    override fun randomInPlace(random: SplittableRandom): VectorValue<DoubleArray> {
        Arrays.setAll(this.value) { Double.fromBits(random.nextLong()) }
        return this
    }

    override fun plusInPlace(other: VectorValue<*>): Complex64VectorValue {
        Arrays.setAll(this.value) { this.value[it] + other.getAsDouble(it) }
        return this
    }

    override fun minusInPlace(other: VectorValue<*>): Complex64VectorValue {
        Arrays.setAll(this.value) { this.value[it] - other.getAsDouble(it) }
        return this
    }

    override fun timesInPlace(other: VectorValue<*>): Complex64VectorValue {
        (0 until this.size step 2).forEach {
            this.value[it] = this.value[it] * other.getAsDouble(it) - this.value[it+1] * other.getAsDouble(it)
            this.value[it+1] = this.value[it] * other.getAsDouble(it+1) + this.value[it+1] * other.getAsDouble(it)
        }
        return this
    }

    override fun divInPlace(other: VectorValue<*>): Complex64VectorValue {
        (0 until this.size step 2).forEach {
            val div = (this.value[it+1].pow(2) + other.getAsDouble(it+1).pow(2))
            this.value[it] = (this.value[it] * other.getAsDouble(it) + this.value[it+1] * other.getAsDouble(it)) / div
            this.value[it+1] = (this.value[it+1] * other.getAsDouble(it) - this.value[it] * other.getAsDouble(it+1)) / div
        }
        return this
    }

    override fun plusInPlace(other: Number): Complex64VectorValue {
        (0 until this.size step 2).forEach {
            this.value[it] += other.toDouble()
        }
        return this
    }

    override fun minusInPlace(other: Number): Complex64VectorValue {
        (0 until this.size step 2).forEach {
            this.value[it] -= other.toDouble()
        }
        return this    }

    override fun timesInPlace(other: Number): Complex64VectorValue {
        (0 until this.size step 2).forEach {
            this.value[it] *= other.toDouble()
        }
        return this    }

    override fun divInPlace(other: Number): Complex64VectorValue {
        (0 until this.size step 2).forEach {
            this.value[it] /= other.toDouble()
        }
        return this
    }

    override fun powInPlace(x: Int): Complex64VectorValue {
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

    override fun sqrtInPlace(): Complex64VectorValue {
        (0 until this.size step 2).forEach {
            this.value[it] = kotlin.math.sqrt((this.value[it] + kotlin.math.sqrt(this.value[it].pow(2) + this.value[it+1].pow(2)))/2)
            this.value[it+1] = (this.value[it+1] / kotlin.math.abs(this.value[it+1])) * kotlin.math.sqrt((- this.value[it] + kotlin.math.sqrt(this.value[it].pow(2) + this.value[it+1].pow(2)))/2)
        }
        return this
    }

    override fun absInPlace(): Complex64VectorValue {
        Arrays.setAll(this.value) { kotlin.math.abs(this.value[it]) }
        return this
    }

    override fun componentsEqual(other: VectorValue<*>): VectorValue<DoubleArray> = Complex64VectorValue(DoubleArray(this.value.size) { if (this.value[it] == other.getAsDouble(it)) { 1.0 } else { 0.0 } })

    override fun sum(): Double = (0 until this.size step 2).map { kotlin.math.sqrt(this.value[it].pow(2) + this.value[it].pow(2)) }.sum()

}