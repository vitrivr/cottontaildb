package org.vitrivr.cottontail.dbms.statistics.statData

import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.selectivity.Selectivity

/**
 * A basic implementation of a [DataMetrics] object, which is used by Cottontail DB to store
 * statistics about [Value]s it encounters.
 *
 * These classes collect statistics about columns, which the query planner can use to make informed decisions
 * about how to execute a query.
 *
 * @author Ralph Gasser, Florian Burkhardt
 * @version 1.3.0
 */
sealed class AbstractValueMetrics<T : Value>(override val type: Types<T>) : DataMetrics<T> {
    companion object {
        const val FRESH_KEY = "fresh"
        const val ENTRIES_KEY = "entries"
        const val NULL_ENTRIES_KEY = "null_entries"
    }

    /** Number of null entries known to this [AbstractValueMetrics]. */
    override var numberOfNullEntries: Long = 0L
        protected set

    /** Number of non-null entries known to this [AbstractValueMetrics]. */
    override var numberOfNonNullEntries: Long = 0L
        protected set

    /** Total number of entries known to this [AbstractValueMetrics]. */
    override val numberOfEntries
        get() = this.numberOfNullEntries + this.numberOfNonNullEntries

    /** Total number of distinct entries known to this [AbstractValueMetrics]. */
    override var numberOfDistinctEntries: Long = 0L
        protected set

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
        FRESH_KEY to this.fresh.toString(),
        NULL_ENTRIES_KEY to this.numberOfNullEntries.toString(),
        ENTRIES_KEY to (this.numberOfNullEntries + this.numberOfNonNullEntries).toString()
    )

    /**
     * Estimates [Selectivity] of the given [BooleanPredicate.Atomic], i.e., the percentage of [org.vitrivr.cottontail.core.basics.Record]s that match it.
     * Defaults to [Selectivity.DEFAULT] but can be overridden by concrete implementations.
     *
     * @param predicate [BooleanPredicate.Atomic] To estimate [Selectivity] for.
     * @return [Selectivity] estimate.
     */
    context(BindingContext, Record)
    override fun estimateSelectivity(predicate: BooleanPredicate.Atomic): Selectivity = when(val operator = predicate.operator){

        // For when it's equal (==) or not-equal (!=)
        is ComparisonOperator.Binary.Equal -> if (predicate.not) {
            Selectivity(this.numberOfEntries.toFloat() * (this.numberOfDistinctEntries.toFloat() - 1f) / this.numberOfDistinctEntries.toFloat()) //Case Not-equals (like A != 10)
        } else {
            Selectivity(this.numberOfEntries.toFloat() / this.numberOfDistinctEntries.toFloat()) // Case Equality =
        } // TODO unclear from book. Is a not (A=10) different to A!=10? 2 different formulas

        // For when it's an inequality with greater or less than a constant (potentially with/without equal >=)
        is ComparisonOperator.Binary.Greater, is ComparisonOperator.Binary.Less, is ComparisonOperator.Binary.GreaterEqual -> if (predicate.not) {
            Selectivity(1f - this.numberOfEntries.toFloat() / 3f) //Not ((Greater or GreaterEqual or Less or LessEqual) (like NOT( A >= 10))
        } else {
            Selectivity(this.numberOfEntries.toFloat() / 3f) // "Greater or GreaterEqual, like A >= 0
        }

        // Between case. Assumption: will return fewer tuples than a less/greater operator (since it's a composition of less and greater.
        is ComparisonOperator.Between -> if (predicate.not) {
            Selectivity(1f - this.numberOfEntries.toFloat() / 6f)
        } else {
            Selectivity(this.numberOfEntries.toFloat() / 6f) //
        }

        /* This can actually be calculated exactly. */
        is ComparisonOperator.IsNull -> if (predicate.not) {
            Selectivity(this.numberOfNonNullEntries.toFloat() / this.numberOfEntries.toFloat())
        } else {
            Selectivity(this.numberOfNullEntries.toFloat() / this.numberOfEntries.toFloat())
        }

        /* Assumption: All elements in IN are matches. */
        is ComparisonOperator.In -> if (predicate.not) {
            Selectivity((this.numberOfEntries - operator.right.size).toFloat() / this.numberOfEntries.toFloat())
        } else {
            Selectivity(operator.right.size / this.numberOfEntries.toFloat())
        }

        // Default for LIKE and MATCH
        else -> Selectivity.DEFAULT
    }
}
