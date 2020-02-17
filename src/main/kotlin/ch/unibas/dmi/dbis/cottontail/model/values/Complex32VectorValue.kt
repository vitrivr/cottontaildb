package ch.unibas.dmi.dbis.cottontail.model.values

/**
 * This is an abstraction over an [Array] and it represents a vector of [Complex32]s.
 *
 * @author Manuel Huerbin
 * @version 1.0
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
    override fun copy(): VectorValue<FloatArray> = Complex32VectorValue(value.copyOf())


    override fun plus(other: VectorValue<FloatArray>): VectorValue<FloatArray> {
        assert(this.size == other.size)
        return Complex32VectorValue(FloatArray(this.size) {
            this.value[it] + other.value[it]
        })
    }

    override fun minus(other: VectorValue<FloatArray>): VectorValue<FloatArray> {
        assert(this.size == other.size)
        return Complex32VectorValue(FloatArray(this.size) {
            this.value[it] - other.value[it]
        })
    }

    override fun times(other: VectorValue<FloatArray>): VectorValue<FloatArray> {
        assert(this.size == other.size)
        return Complex32VectorValue(FloatArray(this.size) {
            if (it % 2 == 0) {
                this.value[it] * other.value[it] - this.value[it+1] * other.value[it+1]
            } else {
                this.value[it-1] * other.value[it] + this.value[it] * other.value[it-1]
            }
        })
    }

    override fun div(other: VectorValue<FloatArray>): VectorValue<FloatArray> {
        assert(this.size == other.size)
        return Complex32VectorValue(FloatArray(this.size) {
            if (it % 2 == 0) {
                (this.value[it] * other.value[it] + this.value[it+1] * other.value[it+1]) / (other.value[it] * other.value[it] + other.value[it+1] * other.value[it+1])
            } else {
                (this.value[it] * other.value[it-1] - this.value[it-1] * other.value[it]) / (other.value[it-1] * other.value[it-1] + other.value[it] * other.value[it])
            }
        })
    }

    override fun plus(other: Number): VectorValue<FloatArray> = Complex32VectorValue(FloatArray(this.size) {
        if (it % 2 == 0) {
            this.value[it] + other.toFloat()
        } else {
            this.value[it]
        }
    })

    override fun minus(other: Number): VectorValue<FloatArray> = Complex32VectorValue(FloatArray(this.size) {
        if (it % 2 == 0) {
            this.value[it] - other.toFloat()
        } else {
            this.value[it]
        }
    })

    override fun times(other: Number): VectorValue<FloatArray> = Complex32VectorValue(FloatArray(this.size) {
        if (it % 2 == 0) {
            this.value[it] * other.toFloat()
        } else {
            this.value[it]
        }
    })

    override fun div(other: Number): VectorValue<FloatArray> = Complex32VectorValue(FloatArray(this.size) {
        if (it % 2 == 0) {
            this.value[it] / other.toFloat()
        } else {
            this.value[it]
        }
    })
}