package org.vitrivr.cottontail.core.queries.planning.cost

import kotlin.math.max

/**
 * Represents a unit of [NormalizedCost]. Used to measure and compare operations in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@JvmInline
value class NormalizedCost(private val cost: FloatArray) {

    companion object {
        /**
         * Normalizes the provided list of [Cost]s w.r.t to the worst-case [Cost].
         *
         * @param list The [List] of [Cost]s to normalize.
         * @return Normalized [List] of [NormalizedCost]
         */
        fun normalize(list: List<Cost>) : List<NormalizedCost> {
            val maxCost = floatArrayOf(0.0f, 0.0f, 0.0f, 0.0f)
            for (c in list) {
                maxCost[0] = max(maxCost[0], c.io)
                maxCost[1] = max(maxCost[1], c.cpu)
                maxCost[2] = max(maxCost[2], c.memory)
                maxCost[3] = max(maxCost[3], c.accuracy)
            }
            return list.map {
                NormalizedCost(
                    if (maxCost[0] > 0.0f) { it.io / maxCost[0] } else { 0.0f },
                    if (maxCost[1] > 0.0f) { it.cpu / maxCost[1] } else { 0.0f },
                    if (maxCost[2] > 0.0f) { it.memory / maxCost[2] } else { 0.0f },
                    if (maxCost[3] > 0.0f) { it.accuracy / maxCost[3] } else { 0.0f },
                )
            }
        }
    }

    /**
     * Default constructor for [NormalizedCost] object.
     *
     * @param io The IO dimension of the [NormalizedCost] object.
     * @param cpu The CPU dimension of the [NormalizedCost] object.
     * @param memory The Memory dimension of the [NormalizedCost] object.
     * @param accuracy The Accuracy dimension of the [NormalizedCost] object.
     */
    constructor(io: Float = 0.0f, cpu: Float = 0.0f, memory: Float = 0.0f, accuracy: Float = 0.0f): this(floatArrayOf(io, cpu, memory, accuracy))

    init {
        require(this.cost[0] in 0.0f..1.0f) { "Only values between 0 and 1.0 are allowed for normalised costs." }
        require(this.cost[1] in 0.0f..1.0f) { "Only values between 0 and 1.0 are allowed for normalised costs." }
        require(this.cost[2] in 0.0f..1.0f) { "Only values between 0 and 1.0 are allowed for normalised costs." }
        require(this.cost[3] in 0.0f..1.0f) { "Only values between 0 and 1.0 are allowed for normalised costs." }
    }

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
}