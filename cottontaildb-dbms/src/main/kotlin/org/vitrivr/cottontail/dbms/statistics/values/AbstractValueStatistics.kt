package org.vitrivr.cottontail.dbms.statistics.values

import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.statistics.selectivity.Selectivity

/**
 * A basic implementation of a [ValueStatistics] object, which is used by Cottontail DB to store
 * statistics about [Value]s it encounters.
 *
 * These classes collect statistics about columns, which the query planner can use to make informed decisions
 * about how to execute a query.
 *
 * @author Florian Burkhardt
 * @author Ralph Gasser
 * @version 1.3.0
 */
sealed class AbstractValueStatistics<T : Value>(override val type: Types<T>) : ValueStatistics<T> {
    companion object {
        const val ENTRIES_KEY = "entries"
        const val NULL_ENTRIES_KEY = "null_entries"
        const val NON_NULL_ENTRIES_KEY = "non_null_entries"
        const val DISTINCT_ENTRIES = "distinct_entries"
    }

    /** Number of null entries known to this [AbstractValueStatistics]. */
    override var numberOfNullEntries: Long = 0L

    /** Number of non-null entries known to this [AbstractValueStatistics]. */
    override var numberOfNonNullEntries: Long = 0L

    /** Total number of entries known to this [AbstractValueStatistics]. */
    override val numberOfEntries
        get() = this.numberOfNullEntries + this.numberOfNonNullEntries

    /** Total number of distinct entries known to this [AbstractScalarStatistics]. */
    override var numberOfDistinctEntries: Long = 0L

    /** Smallest [Value] seen in terms of space requirement (logical size) known to this [AbstractValueStatistics]. */
    override val minWidth: Int
        get() = this.type.logicalSize

    /** Largest [Value] in terms of space requirement (logical size) known to this [AbstractValueStatistics] */
    override val maxWidth: Int
        get() = this.type.logicalSize

    /** Mean [Value] in terms of space requirement (logical size) known to this [AbstractValueStatistics] */
    override val avgWidth: Int
        get() = (this.minWidth + this.maxWidth) / 2

    /** A threshold that defines the ratio between distinct entries and number of entries at which we start to scale when going from the sample size to the population size */
    override val distinctEntriesScalingThreshold: Float = 0.3f

    /**
     * Creates a descriptive map of this [AbstractValueStatistics].
     *
     * @return Descriptive map of this [AbstractValueStatistics]
     */
    override fun about(): Map<String, String> = mapOf(
        NULL_ENTRIES_KEY to this.numberOfNullEntries.toString(),
        NON_NULL_ENTRIES_KEY to this.numberOfNonNullEntries.toString(),
        ENTRIES_KEY to (this.numberOfNullEntries + this.numberOfNonNullEntries).toString(),
        DISTINCT_ENTRIES to this.numberOfDistinctEntries.toString()
    )

    /**
     * Estimates [Selectivity] of the given [BooleanPredicate.Comparison], i.e., the percentage of [Tuple]s that match it.
     * Defaults to [Selectivity.DEFAULT] but can be overridden by concrete implementations.
     *
     * @param predicate [BooleanPredicate.Comparison] To estimate [Selectivity] for.
     * @return [Selectivity] estimate.
     */
    context(BindingContext, Tuple)
    override fun estimateSelectivity(predicate: BooleanPredicate.Comparison): Selectivity {
        val selected = when(val operator = predicate.operator){
            /* =: Textbook definition using estimate for number of distinct entries. */
            is ComparisonOperator.Equal ->  this.numberOfEntries.toFloat() / this.numberOfDistinctEntries.toFloat()

            /* !=: Textbook definition using estimate for number if distinct entries. */
            is ComparisonOperator.NotEqual ->this.numberOfEntries.toFloat() * (this.numberOfDistinctEntries.toFloat() - 1f) / this.numberOfDistinctEntries.toFloat() //Case Not-equals (like A != 10)

            /* <, >, <=, >=: Textbook definition. Assumption is, that 1/3 of the collection is selected (see [1]) */
            is ComparisonOperator.Greater,
            is ComparisonOperator.Less,
            is ComparisonOperator.GreaterEqual,
            is ComparisonOperator.LessEqual -> this.numberOfEntries.toFloat() / 3f

            /* BETWEEN: Assumption is, that it returns fewer tuples than a less/greater operator (since it's a composition of both using AND). */
            is ComparisonOperator.Between ->  this.numberOfEntries.toFloat() / 6f //

            /* IN: Assumption is, that all elements IN are matches (worst-case). */
            is ComparisonOperator.In -> (operator.right.size() * this.numberOfEntries.toFloat()) / this.numberOfDistinctEntries.toFloat()

            // Default case:
            else -> this.numberOfEntries / 3f
        }
        return Selectivity(selected / this.numberOfEntries.toFloat())
    }
}
