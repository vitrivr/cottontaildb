package org.vitrivr.cottontail.core.queries

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
    @JvmInline
    value class CostPolicy(private val weights: FloatArray): QueryHint, org.vitrivr.cottontail.core.queries.planning.cost.CostPolicy {
        /**
         * Default constructor for [CostPolicy] object.
         *
         * @param wio The IO weight of the [CostPolicy] object.
         * @param wcpu The CPU weight of the [CostPolicy] object.
         * @param wmemory The memory weight of the [CostPolicy] object.
         * @param waccuracy The accuracy weight of the [CostPolicy] object.
         */
        constructor(wio: Float, wcpu: Float, wmemory: Float, waccuracy: Float): this(floatArrayOf(wio, wcpu, wmemory, waccuracy))

        override val wio: Float
            get() = this.weights[0]

        override val wcpu: Float
            get() = this.weights[1]

        override val wmemory: Float
            get() = this.weights[2]

        override val waccuracy: Float
            get() = this.weights[3]
    }
}