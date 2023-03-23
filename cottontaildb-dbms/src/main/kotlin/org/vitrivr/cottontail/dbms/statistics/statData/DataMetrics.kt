package org.vitrivr.cottontail.dbms.statistics.statData

import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.selectivity.Selectivity

/**
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.1.0
 */
sealed interface DataMetrics<T : Value> {

    /** The [Types] of [DataMetrics]. */
    val type: Types<T>

    /** Number of null entries known to this [DataMetrics]. */
    val numberOfNullEntries: Long

    /** Number of non-null entries known to this [DataMetrics]. */
    val numberOfNonNullEntries: Long

    /** Total number of entries known to this [DataMetrics]. */
    val numberOfEntries: Long

    /** Total number of distinct entries known to this [DataMetrics]. */
    val numberOfDistinctEntries: Long

    /** Smallest [Value] seen in terms of space requirement (logical size) known to this [DataMetrics]. */
    val minWidth: Int

    /** Largest [Value] in terms of space requirement (logical size) known to this [DataMetrics] */
    val maxWidth: Int

    /** Mean [Value] in terms of space requirement (logical size) known to this [DataMetrics] */
    val avgWidth: Int

    /**
     * Creates a descriptive map of this [DataMetrics].
     *
     * @return Descriptive map of this [DataMetrics]
     */
    fun about(): Map<String,String>

    /**
     * Resets this [DataMetrics] and sets all its values to the default value.
     */
    fun reset()

    /**
     * Estimates [Selectivity] of the given [BooleanPredicate.Atomic], i.e., the percentage of [org.vitrivr.cottontail.core.basics.Record]s that match it.
     * Defaults to [Selectivity.DEFAULT] but can be overridden by concrete implementations.
     *
     * @param predicate [BooleanPredicate.Atomic] To estimate [Selectivity] for.
     * @return [Selectivity] estimate.
     */
    context(BindingContext,Record)
    fun estimateSelectivity(predicate: BooleanPredicate.Atomic): Selectivity
}