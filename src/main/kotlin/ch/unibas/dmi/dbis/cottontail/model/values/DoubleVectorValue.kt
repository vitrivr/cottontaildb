package ch.unibas.dmi.dbis.cottontail.model.values

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
    override fun getAsBool(i: Int) = this.value[i] == 0.0

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

    override operator fun plus(other: VectorValue<DoubleArray>): VectorValue<DoubleArray> {
        require(this.size == other.size) { "Only vector values of the same type and size can be added." }
        val out = DoubleVectorValue(this.value.copyOf(this.size))
        this.indices.forEach { out.value[it] += other.value[it] }
        return out
    }

    override operator fun minus(other: VectorValue<DoubleArray>): VectorValue<DoubleArray> {
        require(this.size == other.size) { "Only vector values of the same type and size can be added." }
        val out = DoubleVectorValue(this.value.copyOf(this.size))
        this.indices.forEach { out.value[it] -= other.value[it] }
        return out
    }

    override operator fun times(other: VectorValue<DoubleArray>): VectorValue<DoubleArray> {
        require(this.size == other.size) { "Only vector values of the same type and size can be added." }
        val out = DoubleVectorValue(this.value.copyOf(this.size))
        this.indices.forEach { out.value[it] *= other.value[it] }
        return out
    }

    override operator fun div(other: VectorValue<DoubleArray>): VectorValue<DoubleArray> {
        require(this.size == other.size) { "Only vector values of the same type and size can be added." }
        val out = DoubleVectorValue(this.value.copyOf(this.size))
        this.indices.forEach { out.value[it] /= other.value[it] }
        return out
    }

    override operator fun plus(other: Number): VectorValue<DoubleArray> {
        val value = other.toDouble()
        val out = DoubleVectorValue(this.value.copyOf(this.size))
        this.indices.forEach { out.value[it] += value }
        return out
    }

    override operator fun minus(other: Number): VectorValue<DoubleArray> {
        val value = other.toDouble()
        val out = DoubleVectorValue(this.value.copyOf(this.size))
        this.indices.forEach { out.value[it] -= value }
        return out
    }

    override operator fun times(other: Number): VectorValue<DoubleArray> {
        val value = other.toDouble()
        val out = DoubleVectorValue(this.value.copyOf(this.size))
        this.indices.forEach { out.value[it] *= value }
        return out
    }

    override operator fun div(other: Number): VectorValue<DoubleArray> {
        val value = other.toDouble()
        val out = DoubleVectorValue(this.value.copyOf(this.size))
        this.indices.forEach { out.value[it] /= value }
        return out
    }
}