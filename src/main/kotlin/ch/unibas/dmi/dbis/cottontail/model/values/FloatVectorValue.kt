package ch.unibas.dmi.dbis.cottontail.model.values

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
    override fun getAsBool(i: Int) = this.value[i] == 0.0f

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

    override operator fun plus(other: VectorValue<FloatArray>): VectorValue<FloatArray> {
        require(this.size == other.size) { "Only vector values of the same type and size can be added." }
        val out = FloatVectorValue(this.value.copyOf(this.size))
        this.indices.forEach { out.value[it] += other.value[it] }
        return out
    }

    override operator fun minus(other: VectorValue<FloatArray>): VectorValue<FloatArray> {
        require(this.size == other.size) { "Only vector values of the same type and size can be added." }
        val out = FloatVectorValue(this.value.copyOf(this.size))
        this.indices.forEach { out.value[it] -= other.value[it] }
        return out
    }

    override operator fun times(other: VectorValue<FloatArray>): VectorValue<FloatArray> {
        require(this.size == other.size) { "Only vector values of the same type and size can be added." }
        val out = FloatVectorValue(this.value.copyOf(this.size))
        this.indices.forEach { out.value[it] *= other.value[it] }
        return out
    }

    override operator fun div(other: VectorValue<FloatArray>): VectorValue<FloatArray> {
        require(this.size == other.size) { "Only vector values of the same type and size can be added." }
        val out = FloatVectorValue(this.value.copyOf(this.size))
        this.indices.forEach { out.value[it] /= other.value[it] }
        return out
    }

    override operator fun plus(other: Number): VectorValue<FloatArray> {
        val value = other.toFloat()
        val out = FloatVectorValue(this.value.copyOf(this.size))
        this.indices.forEach { out.value[it] += value }
        return out
    }

    override operator fun minus(other: Number): VectorValue<FloatArray> {
        val value = other.toFloat()
        val out = FloatVectorValue(this.value.copyOf(this.size))
        this.indices.forEach { out.value[it] -= value }
        return out
    }

    override operator fun times(other: Number): VectorValue<FloatArray> {
        val value = other.toFloat()
        val out = FloatVectorValue(this.value.copyOf(this.size))
        this.indices.forEach { out.value[it] *= value }
        return out
    }

    override operator fun div(other: Number): VectorValue<FloatArray> {
        val value = other.toFloat()
        val out = FloatVectorValue(this.value.copyOf(this.size))
        this.indices.forEach { out.value[it] /= value }
        return out
    }
}