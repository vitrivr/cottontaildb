package org.vitrivr.cottontail.core.queries.planning.cost

/**
 * A [CostPolicy] that can be used to transform a [Cost] into a score and to compare [Cost]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface CostPolicy: Comparator<NormalizedCost> {

    /** The weight / importance of the IO aspect of the [CostPolicy]. */
    val wio: Float

    /** The weight/ importance  of the CPU aspect of the [CostPolicy]. */
    val wcpu: Float

    /** The weight / importance of the memory aspect of the [CostPolicy]. */
    val wmemory: Float

    /** The weight / importance of the accuracy aspect of the [CostPolicy]. */
    val waccuracy: Float

    /**
     * The desired speedup per additional worker of [CostPolicy].
     *
     * This value determines how many workers are used for intra query parallelism.
     */
    val speedupPerWorker: Float

    /** The non-parallelisable fraction of IO work. This is usually higher for HDDs than SSDs. */
    val parallelisableIO: Float

    /**
     * Transforms the given [Cost] object into a cost score given this [CostPolicy].
     *
     * The score is used to directly compare two [Cost] objects
     *
     * @param cost The [Cost] to transform.
     * @return The cost score.
     */
    fun toScore(cost: NormalizedCost): Float =
        (this.wio * cost.io + this.wcpu * cost.cpu + this.wmemory * cost.memory + this.waccuracy * cost.accuracy)

    /**
     * Compares two [NormalizedCost] objects based on the score.
     *
     * @param o1 The first [NormalizedCost] in the comparison.
     * @param o2 The second [NormalizedCost] in the comparison.
     * @return
     */
    override fun compare(o1: NormalizedCost, o2: NormalizedCost): Int = this.toScore(o1).compareTo(this.toScore(o2))

    /**
     * Estimates, how much parallelization makes sense given the parallelisable portion vs the total [Cost]. The estimation
     * is done by applying Amdahl's law. See https://en.wikipedia.org/wiki/Amdahl%27s_law
     *
     * @param parallelisableCost The parallelisable portion of the total [Cost]
     * @param totalCost The total [Cost]
     * @param pmax The maximum amount of parallelisation.
     * @return parallelization estimation for this [Cost].
     */
    fun parallelisation(parallelisableCost: Cost, totalCost: Cost, pmax: Int): Int {
        if (pmax < 2) return 1
        if (parallelisableCost.cpu < 1.0f) return 1
        val sp = parallelisableCost.cpu + parallelisableCost.io * this.parallelisableIO /* Parallelisable portion of the cost. */
        val ss = (totalCost.cpu - parallelisableCost.cpu) + (totalCost.io - parallelisableCost.io * this.parallelisableIO) /* Serial portion of the cost. */
        val ov = 0.01f * parallelisableCost.cpu /* Overhead = 0.1% of the parallel cost. */
        var prevSpeedup = 0.0f
        for (p in 2 .. pmax) {
            val s = (ss + sp) / (ss + (sp / p) + p * ov)
            val ds = s - prevSpeedup
            if (ds < this.speedupPerWorker) return (p - 1)
            prevSpeedup = s
        }
        return pmax
    }
}