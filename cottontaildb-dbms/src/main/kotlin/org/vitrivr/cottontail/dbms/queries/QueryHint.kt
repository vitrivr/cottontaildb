package org.vitrivr.cottontail.dbms.queries

import org.vitrivr.cottontail.dbms.index.IndexType
import kotlin.math.abs

/**
 * A [QueryHint] as provided by the user that issues a query.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
interface QueryHint {
    /**
     * A [QueryHint] that instructs the query planner use no index.
     */
    object NoIndex: QueryHint

    /**
     * A [QueryHint] that instructs the query planner use a specific index
     */
    data class Index(val name: String? = null, val type: IndexType? = null): QueryHint {
        /**
         * Checks if the provided [Index] matches thins [QueryHint.Index].
         *
         * @param index The [Index] to check.
         * @return True on success, false otherwise.
         */
        fun matches(index: org.vitrivr.cottontail.dbms.index.Index): Boolean {
            if (this.name != null && index.name.simple != this.name) {
                return false
            }
            if (this.type != null && index.type != this.type) {
                return false
            }
            return true
        }
    }

    /**
     * A [QueryHint] that instructs the query execution engine to limit parallelism.
     */
    data class Parallelism(val max: Int): QueryHint

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