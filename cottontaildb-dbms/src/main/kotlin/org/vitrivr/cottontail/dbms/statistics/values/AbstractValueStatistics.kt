package org.vitrivr.cottontail.dbms.statistics.values

import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.statistics.selectivity.Selectivity

/**
 * A basic implementation of a [AbstractValueStatistics] object, which is used by Cottontail DB to collect and summary
 * statistics about [Value]s it encounters.
 *
 * These classes collect statistics about columns, which the query planner can use to make informed decisions
 * about how to execute a query.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
sealed class AbstractValueStatistics<T : Value>(override val type: Types<T>): ValueStatistics<T> {
    companion object {
        const val FRESH_KEY = "fresh"
        const val ENTRIES_KEY = "entries"
        const val NULL_ENTRIES_KEY = "null_entries"
    }

    /** Flag indicating that this [AbstractValueStatistics] needs updating. */
    override var fresh: Boolean = true
        protected set

    /** Number of null entries known to this [AbstractValueStatistics]. */
    override var numberOfNullEntries: Long = 0L
        protected set

    /** Number of non-null entries known to this [AbstractValueStatistics]. */
    override var numberOfNonNullEntries: Long = 0L
        protected set

    /** Total number of entries known to this [AbstractValueStatistics]. */
    override val numberOfEntries
        get() = this.numberOfNullEntries + this.numberOfNonNullEntries

    /** Smallest [Value] seen in terms of space requirement (logical size) known to this [AbstractValueStatistics]. */
    override val minWidth: Int
        get() = this.type.logicalSize

    /** Largest [Value] in terms of space requirement (logical size) known to this [AbstractValueStatistics] */
    override val maxWidth: Int
        get() = this.type.logicalSize

    /** Mean [Value] in terms of space requirement (logical size) known to this [AbstractValueStatistics] */
    override val avgWidth: Int
        get() = (this.minWidth + this.maxWidth) / 2

    /**
     * Updates this [AbstractValueStatistics] with an inserted [Value]
     *
     * @param inserted The [Value] that was deleted.
     */
    @Synchronized
    override fun insert(inserted: T?) {
        if (inserted == null) {
            this.numberOfNullEntries += 1
        } else {
            this.numberOfNonNullEntries += 1
        }
    }

    /**
     * Updates this [AbstractValueStatistics] with a deleted [Value]
     *
     * @param deleted The [Value] that was deleted.
     */
    @Synchronized
    override fun delete(deleted: T?) {
        if (deleted == null) {
            this.numberOfNullEntries -= 1
        } else {
            this.numberOfNonNullEntries -= 1
        }
    }

    /**
     * Updates this [AbstractValueStatistics] with a new updated value [T].
     *
     * Default implementation is simply a combination of [insert] and [delete].
     *
     * @param old The [Value] before the update.
     * @param new The [Value] after the update.
     */
    @Synchronized
    override fun update(old: T?, new: T?) {
        this.delete(old)
        this.insert(new)
    }

    /**
     * Resets this [AbstractValueStatistics] and sets all its values to the default value.
     */
    @Synchronized
    override fun reset() {
        this.fresh = true
        this.numberOfNullEntries = 0L
        this.numberOfNonNullEntries = 0L
    }

    /**
     * Creates a descriptive map of this [AbstractValueStatistics].
     *
     * @return Descriptive map of this [AbstractValueStatistics]
     */
    override fun about(): Map<String, String> = mapOf(
        FRESH_KEY to this.fresh.toString(),
        NULL_ENTRIES_KEY to this.numberOfNullEntries.toString(),
        ENTRIES_KEY to (this.numberOfNullEntries + this.numberOfNonNullEntries).toString()
    )

    /**
     * Estimates [Selectivity] of the given [BooleanPredicate.Comparison], i.e., the percentage of [org.vitrivr.cottontail.core.basics.Record]s that match it.
     * Defaults to [Selectivity.DEFAULT] but can be overridden by concrete implementations.
     *
     * @param predicate [BooleanPredicate.Comparison] To estimate [Selectivity] for.
     * @return [Selectivity] estimate.
     */
    context(BindingContext,Record)
    override fun estimateSelectivity(predicate: BooleanPredicate.Comparison): Selectivity = when(val operator = predicate.operator){
        /* Assumption: All elements in IN are matches. */
        is ComparisonOperator.In -> Selectivity(operator.right.size() / this.numberOfEntries.toFloat())
        else -> Selectivity.DEFAULT
    }
}