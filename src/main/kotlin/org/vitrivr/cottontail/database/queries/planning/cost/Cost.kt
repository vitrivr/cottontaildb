package org.vitrivr.cottontail.database.queries.planning.cost

/**
 * Represents a unit of [Cost]. Used to measure and compare operations in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
@JvmInline
value class Cost private constructor(private val cost: FloatArray) : Comparable<Cost> {

    companion object {
        val ZERO = Cost(0.0f, 0.0f, 0.0f)
        val INVALID = Cost(Float.NaN, Float.NaN, Float.NaN)

        /** Constant used to estimate, how much parallelization makes sense given CPU [Cost]s. This is a magic number :-) */
        private const val MAX_PARALLELISATION = 4

        /** Cost read access to disk. TODO: Estimate based on local hardware. */
        const val COST_DISK_ACCESS_READ = 1e-4f

        /** Cost read access to disk. TODO: Estimate based on local hardware. */
        const val COST_DISK_ACCESS_WRITE = 5 * 1e-4f

        /** Estimated cost of memory access. */
        val COST_MEMORY_ACCESS = AtomicCostEstimator.estimateAtomicMemoryAccessCost()

        /** Estimated cost of a floating point operation. */
        val COST_FLOP = AtomicCostEstimator.estimateAtomicFlopCost()
    }

    init {

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

    operator fun plus(other: Cost): Cost = Cost(this.io + other.io, this.cpu + other.cpu, this.memory + other.memory, this.accuracy + other.accuracy)
    operator fun minus(other: Cost): Cost = Cost(this.io - other.io, this.cpu - other.cpu, this.memory - other.memory, this.accuracy - other.accuracy)
    operator fun plus(other: Number): Cost = Cost(this.io + other.toFloat(), this.cpu + other.toFloat(), this.memory + other.toFloat(), this.accuracy + other.toFloat())
    operator fun minus(other: Number): Cost = Cost(this.io - other.toFloat(), this.cpu - other.toFloat(), this.memory - other.toFloat(), this.accuracy - other.toFloat())
    operator fun times(other: Number): Cost = Cost(this.io * other.toFloat(), this.cpu * other.toFloat(), this.memory * other.toFloat(), this.accuracy * other.toFloat())
    operator fun div(other: Number): Cost = Cost(this.io / other.toFloat(), this.cpu / other.toFloat(), this.memory / other.toFloat(), this.accuracy / other.toFloat())

    /**
     * Compares this [Cost] to another [Cost] based on the overall score and return a negative number, zero or a
     * positive number of this [Cost] is smaller, equal or greater than the other [Cost].
     *
     * @param other The [Cost] to this [Cost] to.
     */
    override fun compareTo(other: Cost): Int
        = (0.6f * (this.io - other.io) + 0.2f * (this.cpu - other.cpu) + 0.1f * (this.io - other.io) + 0.1f *  (this.accuracy - other.accuracy)).toInt()
}