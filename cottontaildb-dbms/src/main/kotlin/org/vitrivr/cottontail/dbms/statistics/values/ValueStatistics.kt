package org.vitrivr.cottontail.dbms.statistics.values

import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.statistics.selectivity.Selectivity

/**
 * A metric describing a collection of [Value]s. Used by the Cottontail DB statistics management layer.
 *
 * @author Florian Burkhardt
 * @author Ralph Gasser
 * @version 1.1.0
 */
sealed interface ValueStatistics<T : Value> {

    /** The [Types] of [ValueStatistics]. */
    val type: Types<T>

    /** Number of null entries known to this [ValueStatistics]. */
    val numberOfNullEntries: Long

    /** Number of non-null entries known to this [ValueStatistics]. */
    val numberOfNonNullEntries: Long

    /** Total number of entries known to this [ValueStatistics]. */
    val numberOfEntries: Long

    /** Total number of distinct entries known to this [ValueStatistics]. */
    val numberOfDistinctEntries: Long

    /** Smallest [Value] seen in terms of space requirement (logical size) known to this [ValueStatistics]. */
    val minWidth: Int

    /** Largest [Value] in terms of space requirement (logical size) known to this [ValueStatistics] */
    val maxWidth: Int

    /** Mean [Value] in terms of space requirement (logical size) known to this [ValueStatistics] */
    val avgWidth: Int

    /** A threshold that defines the ratio between distinct entries and number of entries at which we start to scale when going from the sample size to the population size */
    val distinctEntriesScalingThreshold: Float

    /**
     * Creates a descriptive map of this [ValueStatistics].
     *
     * @return Descriptive map of this [ValueStatistics]
     */
    fun about(): Map<String,String>

    /**
     * Estimates [Selectivity] of the provided [BooleanPredicate.Comparison], i.e., the percentage of [Tuple]s that match it.
     * Defaults to [Selectivity.DEFAULT] but can be overridden by concrete implementations.
     *
     * @param predicate [BooleanPredicate.Comparison] to estimate [Selectivity] for.
     * @return [Selectivity] estimate.
     */
    context(BindingContext, Tuple)
    fun estimateSelectivity(predicate: BooleanPredicate.Comparison): Selectivity
}