package ch.unibas.dmi.dbis.cottontail.model.values

/**
 * This is an abstraction over an [Array] and it represents a vector of [Complex64]s.
 *
 * @author Manuel Huerbin
 * @version 1.0
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
    override fun copy(): VectorValue<DoubleArray> = Complex64VectorValue(value.copyOf())


    override fun plus(other: VectorValue<DoubleArray>): VectorValue<DoubleArray> {
        assert(this.size == other.size)
        return Complex64VectorValue(DoubleArray(this.size) {
            this.value[it] + other.value[it]
        })
    }

    override fun minus(other: VectorValue<DoubleArray>): VectorValue<DoubleArray> {
        assert(this.size == other.size)
        return Complex64VectorValue(DoubleArray(this.size) {
            this.value[it] - other.value[it]
        })
    }

    override fun times(other: VectorValue<DoubleArray>): VectorValue<DoubleArray> {
        assert(this.size == other.size)
        return Complex64VectorValue(DoubleArray(this.size) {
            if (it % 2 == 0) {
                this.value[it] * other.value[it] - this.value[it + 1] * other.value[it + 1]
            } else {
                this.value[it - 1] * other.value[it] + this.value[it] * other.value[it - 1]
            }
        })
    }

    override fun div(other: VectorValue<DoubleArray>): VectorValue<DoubleArray> {
        assert(this.size == other.size)
        return Complex64VectorValue(DoubleArray(this.size) {
            if (it % 2 == 0) {
                (this.value[it] * other.value[it] + this.value[it + 1] * other.value[it + 1]) / (other.value[it] * other.value[it] + other.value[it + 1] * other.value[it + 1])
            } else {
                (this.value[it] * other.value[it - 1] - this.value[it - 1] * other.value[it]) / (other.value[it - 1] * other.value[it - 1] + other.value[it] * other.value[it])
            }
        })
    }

    override fun plus(other: Number): VectorValue<DoubleArray> = Complex64VectorValue(DoubleArray(this.size) {
        if (it % 2 == 0) {
            this.value[it] + other.toDouble()
        } else {
            this.value[it]
        }
    })

    override fun minus(other: Number): VectorValue<DoubleArray> = Complex64VectorValue(DoubleArray(this.size) {
        if (it % 2 == 0) {
            this.value[it] - other.toDouble()
        } else {
            this.value[it]
        }
    })

    override fun times(other: Number): VectorValue<DoubleArray> = Complex64VectorValue(DoubleArray(this.size) {
        if (it % 2 == 0) {
            this.value[it] * other.toDouble()
        } else {
            this.value[it]
        }
    })

    override fun div(other: Number): VectorValue<DoubleArray> = Complex64VectorValue(DoubleArray(this.size) {
        if (it % 2 == 0) {
            this.value[it] / other.toDouble()
        } else {
            this.value[it]
        }
    })
}