package org.vitrivr.cottontail.dbms.statistics.metricsData

import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.selectivity.Selectivity

/**
 * A [ValueMetrics] implementation for all [Types.Scalar].
 *
 * @author Florian Burkhardt
 * @version 1.0.0
 */
sealed class AbstractScalarMetrics<T: Value>(type: Types<T>): AbstractValueMetrics<T>(type) {

    /** Total number of distinct entries known to this [AbstractScalarMetrics]. */
    var numberOfDistinctEntries: Long = 0L

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