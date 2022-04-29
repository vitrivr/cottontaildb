package org.vitrivr.cottontail.core.queries

import kotlin.math.abs

/**
 * A [QueryHint] as provided by the user that issues a query.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface QueryHint {
    /**
     * A [QueryHint] that instructs the query planner to ignore index structures.
     */
    object NoIndex: QueryHint

    /**
     * A [QueryHint] that instructs the query execution engine to not allow for intra query parallelism.
     */
    object NoParallel: QueryHint

    /**
     * A [QueryHint] that acts as cost [CostPolicy] for query planning.
     */
    data class CostPolicy(
        override val wio: Float,
        override val wcpu: Float,
        override val wmemory: Float,
        override val waccuracy: Float,
        override val speedupPerWorker: Float,
        override val nonParallelisableIO: Float
    ): QueryHint, org.vitrivr.cottontail.core.queries.planning.cost.CostPolicy {
        init {
            require(this.speedupPerWorker in 0.0f .. 1.0f) { "The speedup per worker must lie between 0.0 and 1.0 bit is ${this.speedupPerWorker}."}
            require(this.nonParallelisableIO in 0.0f .. 1.0f) { "The fraction of non-parallelisable IO must lie between 0.0 and 1.0 bit is ${this.nonParallelisableIO}."}
            require( abs((this.wio + this.wcpu + this.wmemory + this.waccuracy) - 1.0f) < 1e-5 ) { "All cost weights must add-up to 1.0 but add up to ${this.wio + this.wcpu + this.wmemory + this.waccuracy}."}
        }
    }
}