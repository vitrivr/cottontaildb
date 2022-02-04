package org.vitrivr.cottontail.core.queries.planning.cost

/**
 * Represents a unit of [Cost]. Used to measure and compare operations in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
@JvmInline
value class Cost private constructor(private val cost: FloatArray) : Comparable<Cost> {

    companion object {
        /** The zero [Cost]. */
        val ZERO: Cost = Cost(0.0f, 0.0f, 0.0f, 0.0f)

        /** [Cost] for an impossible operation. */
        val INVALID: Cost = Cost(Float.NaN, Float.NaN, Float.NaN, Float.NaN)

        /** The estimated [Cost] for accessing a variable in memory. */
        val MEMORY_ACCESS = AtomicCostEstimator.estimateAtomicMemoryAccessCost()

        /** The estimated [Cost] of a single, simple floating point operation (+, -, /, *). */
        val FLOP = AtomicCostEstimator.estimateAtomicMemoryAccessCost()

        /** The estimated [Cost] of a sqrt operation. */
        val OP_SQRT = FLOP * 5.0f

        /** The estimated [Cost] for reading from disk (per word). TODO: Better estimate. */
        val DISK_ACCESS_READ = MEMORY_ACCESS * 1e5f

        /** The estimated [Cost] for writing to disk (per word). TODO: Better estimate. */
        val DISK_ACCESS_WRITE = MEMORY_ACCESS * 4 * 1e5f

        /** Constant used to estimate, how much parallelization makes sense given CPU [Cost]s. This is a magic number :-) */
        private const val MAX_PARALLELISATION = 4
    }

    /**
     * Default constructor for [Cost] object.
     *
     * @param io The IO dimension of the [Cost] object.
     * @param cpu The CPU dimension of the [Cost] object.
     * @param memory The Memory dimension of the [Cost] object.
     * @param accuracy The Accuracy dimension of the [Cost] object.
     */
    constructor(io: Float = 0.0f, cpu: Float = 0.0f, memory: Float = 0.0f, accuracy: Float = 0.0f): this(floatArrayOf(io, cpu, memory, accuracy))

    /** The IO dimension of the [Cost] object. */
    val io: Float
        get() = this.cost[0]

    /** The CPU dimension of the [Cost] object. */
    val cpu: Float
        get() = this.cost[1]

    /** The Memory dimension of the [Cost] object. */
    val memory: Float
        get() = this.cost[2]

    /** The Accuracy dimension of the [Cost] object. */
    val accuracy: Float
        get() = this.cost[3]

    /**
     * Estimates, how much parallelization makes sense given this [Cost].
     *
     * @param max The maximum parallelization to allow.
     * @return parallelization estimation for this [Cost].
     */
    fun parallelisation(max: Int = MAX_PARALLELISATION) = this.cpu.toInt().coerceAtMost(max).coerceAtLeast(1)

    /**
     * Compares this [Cost] to another [Cost] based on the overall score and return a negative number, zero or a
     * positive number of this [Cost] is smaller, equal or greater than the other [Cost].
     *
     * @param other The [Cost] to this [Cost] to.
     */
    override fun compareTo(other: Cost): Int
        = (0.6f * (this.io - other.io) + 0.2f * (this.cpu - other.cpu) + 0.1f * (this.memory - other.memory) + 0.1f *  (this.accuracy - other.accuracy)).toInt()

    operator fun plus(other: Cost): Cost = Cost(this.io + other.io, this.cpu + other.cpu, this.memory + other.memory, this.accuracy + other.accuracy)
    operator fun minus(other: Cost): Cost = Cost(this.io - other.io, this.cpu - other.cpu, this.memory - other.memory, this.accuracy - other.accuracy)
    operator fun plus(other: Number): Cost = Cost(this.io + other.toFloat(), this.cpu + other.toFloat(), this.memory + other.toFloat(), this.accuracy + other.toFloat())
    operator fun minus(other: Number): Cost = Cost(this.io - other.toFloat(), this.cpu - other.toFloat(), this.memory - other.toFloat(), this.accuracy - other.toFloat())
    operator fun times(other: Number): Cost = Cost(this.io * other.toFloat(), this.cpu * other.toFloat(), this.memory * other.toFloat(), this.accuracy * other.toFloat())
    operator fun div(other: Number): Cost = Cost(this.io / other.toFloat(), this.cpu / other.toFloat(), this.memory / other.toFloat(), this.accuracy / other.toFloat())
    operator fun Number.plus(other: Cost) = Cost(other.io + this.toFloat(), other.cpu + this.toFloat(), other.memory + this.toFloat(), other.accuracy + this.toFloat())
    operator fun Number.minus(other: Cost) = Cost(other.io - this.toFloat(), other.cpu - this.toFloat(), other.memory - this.toFloat(), other.accuracy - this.toFloat())
    operator fun Number.times(other: Cost) = Cost(other.io * this.toFloat(), other.cpu * this.toFloat(), other.memory * this.toFloat(), other.accuracy * this.toFloat())
    operator fun Number.div(other: Cost) = Cost(other.io / this.toFloat(), other.cpu / this.toFloat(), other.memory / this.toFloat(), other.accuracy / this.toFloat())
}