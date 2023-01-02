package org.vitrivr.cottontail.config

import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.core.queries.planning.cost.CostPolicy
import kotlin.math.abs

/**
 * Configuration of the default [CostPolicy] for Cottontail DBs' cost model.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@Serializable
data class CostConfig(
    /** The relative weight / importance of IO related costs, w.r.t., to total costs. */
    override val wio: Float = 0.6f,

    /** The relative weight / importance of CPU related costs, w.r.t., to total costs. */
    override val wcpu: Float = 0.3f,

    /** The relative weight / importance of Memory related costs, w.r.t., to total costs. */
    override val wmemory: Float = 0.1f,

    /** The relative weight / importance of Accuracy related costs, w.r.t., to total costs. */
    override val waccuracy: Float = 0.0f,

    /**
     * The desired speedup per additional worker of [CostPolicy]. This value determines how many workers are invested for intra query parallelism.
     *
     * The default value is 0.15f, which means, that Cottontail DB is willing to invest an additional worker for at least 15% expected speedup.
     */
    override val speedupPerWorker: Float = 0.15f,

    /**
     * The fraction of I/O cost Cottontail DB expects to be not parallelisable, e.g., due to limits on I/O bandwith.
     * This value is usually higher for HDDs than SSDs.
     */
    override val nonParallelisableIO: Float = 0.6f
) : CostPolicy {
    init {
        require(this.speedupPerWorker in 0.0f .. 1.0f) { "The speedup per worker must lie between 0.0 and 1.0 bit is ${this.speedupPerWorker}."}
        require(this.nonParallelisableIO in 0.0f .. 1.0f) { "The fraction of non-parallelisable IO must lie between 0.0 and 1.0 bit is ${this.nonParallelisableIO}."}
        require( abs((this.wio + this.wcpu + this.wmemory + this.waccuracy) - 1.0f) < 1e-5 ) { "All cost weights must add-up to 1.0 but add up to ${this.wio + this.wcpu + this.wmemory + this.waccuracy}."}
    }
}