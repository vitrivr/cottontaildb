package org.vitrivr.cottontail.dbms.queries

import org.vitrivr.cottontail.dbms.index.basic.Index

/**
 * A [QueryHint] as provided by the user that issues a query.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
sealed interface QueryHint {

    /**
     * A [QueryHint] that instructs the query execution engine to use / not use certain index structures.
     */
    sealed interface IndexHint: QueryHint {

        /**
         * Checks if the provided [Index] matches this [QueryHint.IndexHint].
         *
         * @param index The [Index] to check.
         * @return True on success, false otherwise.
         */
        fun matches(index: Index): Boolean

        /**
         * A [QueryHint] that instructs the query planner use all available indexes.
         */
        object All: IndexHint {
            override fun matches(index: Index): Boolean = true
        }

        /**
         * A [QueryHint] that instructs the query planner use no index.
         */
        object None: IndexHint {
            override fun matches(index: Index): Boolean = false
        }

        /**
         * A [QueryHint] that instructs the query planner use a specific index
         */
        data class Name(val name: String): IndexHint {
            override fun matches(index: Index): Boolean  = index.name.simple == this.name
        }

        /**
         * A [QueryHint] that instructs the query planner use a specific [IndexType]
         */
        data class Type(val type: org.vitrivr.cottontail.dbms.index.basic.IndexType): IndexHint {
            override fun matches(index: Index): Boolean = index.type == this.type
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
        override val parallelisableIO: Float
    ): QueryHint, org.vitrivr.cottontail.core.queries.planning.cost.CostPolicy {
        init {
            require(this.wio in 0.0f .. 1.0f) { "The IO weight must lie between 0.0 and 1.0 bit is ${this.speedupPerWorker}."}
            require(this.wcpu in 0.0f .. 1.0f) { "The CPU weight must lie between 0.0 and 1.0 bit is ${this.speedupPerWorker}."}
            require(this.wmemory in 0.0f .. 1.0f) { "The Memory weight must lie between 0.0 and 1.0 bit is ${this.speedupPerWorker}."}
            require(this.waccuracy in 0.0f .. 1.0f) { "The quality weight must lie between 0.0 and 1.0 bit is ${this.speedupPerWorker}."}
            require(this.speedupPerWorker in 0.0f .. 1.0f) { "The speedup per worker must lie between 0.0 and 1.0 bit is ${this.speedupPerWorker}."}
            require(this.parallelisableIO in 0.0f .. 1.0f) { "The fraction of non-parallelisable IO must lie between 0.0 and 1.0 bit is ${this.parallelisableIO}."}
        }
    }

    /**
     * A [QueryHint] that instructs the query execution to not perform any plan optimisation.
     */
    object NoOptimisation: QueryHint
}

