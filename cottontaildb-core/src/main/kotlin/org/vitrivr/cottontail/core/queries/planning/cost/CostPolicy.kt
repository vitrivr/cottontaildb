package org.vitrivr.cottontail.core.queries.planning.cost

/**
 * A [CostPolicy] that can be used to transform a [Cost] into a score.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface CostPolicy {
    /** The weight for the IO aspect of the [CostPolicy]. */
    val wio: Float

    /** The weight for the CPU aspect of the [CostPolicy]. */
    val wcpu: Float

    /** The weight for the memory aspect of the [CostPolicy]. */
    val wmemory: Float

    /** The weight for the accuracy aspect of the [CostPolicy]. */
    val waccuracy: Float

    /**
     * Transforms the given [Cost] object into a cost score given this [CostPolicy].
     *
     * @param cost The [Cost] to transform.
     * @return The cost score.
     */
    fun toScore(cost: Cost): Float =
        (this.wio * cost.io + this.wcpu * cost.cpu + this.wmemory * cost.memory + this.waccuracy * cost.accuracy)

}