package org.vitrivr.cottontail.dbms.statistics.metricsData

import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.selectivity.Selectivity

/**
 * A basic implementation of a [ValueMetrics] object, which is used by Cottontail DB to store
 * statistics about [Value]s it encounters.
 *
 * These classes collect statistics about columns, which the query planner can use to make informed decisions
 * about how to execute a query.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
sealed class AbstractValueMetrics<T : Value>(override val type: Types<T>) : ValueMetrics<T> {
    companion object {
        const val FRESH_KEY = "fresh"
        const val ENTRIES_KEY = "entries"
        const val NULL_ENTRIES_KEY = "null_entries"
    }

    /** Number of null entries known to this [AbstractValueMetrics]. */
    override var numberOfNullEntries: Long = 0L
        //protected set

    /** Number of non-null entries known to this [AbstractValueMetrics]. */
    override var numberOfNonNullEntries: Long = 0L
        //protected set

    /** Total number of entries known to this [AbstractValueMetrics]. */
    override val numberOfEntries
        get() = this.numberOfNullEntries + this.numberOfNonNullEntries

    /** Smallest [Value] seen in terms of space requirement (logical size) known to this [AbstractValueMetrics]. */
    override val minWidth: Int
        get() = this.type.logicalSize

    /** Largest [Value] in terms of space requirement (logical size) known to this [AbstractValueMetrics] */
    override val maxWidth: Int
        get() = this.type.logicalSize

    /** Mean [Value] in terms of space requirement (logical size) known to this [AbstractValueMetrics] */
    override val avgWidth: Int
        get() = (this.minWidth + this.maxWidth) / 2

    /**
     * Resets this [AbstractValueMetrics] and sets all its values to the default value.
     */
    @Synchronized
    override fun reset() {
        this.numberOfNullEntries = 0L
        this.numberOfNonNullEntries = 0L
    }

    /**
     * Creates a descriptive map of this [AbstractValueMetrics].
     *
     * @return Descriptive map of this [AbstractValueMetrics]
     */
    override fun about(): Map<String, String> = mapOf(
        //FRESH_KEY to this.fresh.toString(),
        NULL_ENTRIES_KEY to this.numberOfNullEntries.toString(),
        ENTRIES_KEY to (this.numberOfNullEntries + this.numberOfNonNullEntries).toString()
    )


}
