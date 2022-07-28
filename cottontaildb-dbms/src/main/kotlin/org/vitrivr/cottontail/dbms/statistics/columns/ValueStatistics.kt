package org.vitrivr.cottontail.dbms.statistics.columns

import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.selectivity.Selectivity

/**
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed interface ValueStatistics<T : Value> {

    /** The [Types] of [ValueStatistics]. */
    val type: Types<T>

    /** Flag indicating that this [ValueStatistics] needs updating. */
    val fresh: Boolean

    /** Number of null entries known to this [ValueStatistics]. */
    val numberOfNullEntries: Long

    /** Number of non-null entries known to this [ValueStatistics]. */
    val numberOfNonNullEntries: Long

    /** Total number of entries known to this [ValueStatistics]. */
    val numberOfEntries: Long

    /** Smallest [Value] seen in terms of space requirement (logical size) known to this [ValueStatistics]. */
    val minWidth: Int

    /** Largest [Value] in terms of space requirement (logical size) known to this [ValueStatistics] */
    val maxWidth: Int

    /** Mean [Value] in terms of space requirement (logical size) known to this [ValueStatistics] */
    val avgWidth: Int

    /**
     * Updates this [ValueStatistics] with an inserted [Value]
     *
     * @param inserted The [Value] that was deleted.
     */
    fun insert(inserted: T?)

    /**
     * Updates this [ValueStatistics] with a deleted [Value]
     *
     * @param deleted The [Value] that was deleted.
     */
    fun delete(deleted: T?)

    /**
     * Updates this [ValueStatistics] with a new updated value [T].
     *
     * Default implementation is simply a combination of [insert] and [delete].
     *
     * @param old The [Value] before the update.
     * @param new The [Value] after the update.
     */
    fun update(old: T?, new: T?)

    /**
     * Creates a descriptive map of this [ValueStatistics].
     *
     * @return Descriptive map of this [ValueStatistics]
     */
    fun about(): Map<String,String>

    /**
     * Resets this [ValueStatistics] and sets all its values to the default value.
     */
    fun reset()

    /**
     * Copies this [ValueStatistics] and returns it.
     *
     * @return Copy of this [ValueStatistics].
     */
    fun copy(): ValueStatistics<T>

    /**
     * Estimates [Selectivity] of the given [BooleanPredicate.Atomic], i.e., the percentage of [org.vitrivr.cottontail.core.basics.Record]s that match it.
     * Defaults to [Selectivity.DEFAULT] but can be overridden by concrete implementations.
     *
     * @param predicate [BooleanPredicate.Atomic] To estimate [Selectivity] for.
     * @return [Selectivity] estimate.
     */
    context(org.vitrivr.cottontail.core.basics.Record, BindingContext)
    fun estimateSelectivity(predicate: BooleanPredicate.Atomic): Selectivity
}