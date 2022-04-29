package org.vitrivr.cottontail.dbms.statistics.selectivity

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.dbms.statistics.columns.ValueStatistics
import org.vitrivr.cottontail.dbms.statistics.selectivity.Selectivity.Companion.DEFAULT_SELECTIVITY

/**
 * This is a very naive calculator for [Selectivity] values.
 *
 * It simply delegates [Selectivity] calculation for [BooleanPredicate.Atomic]to the [ValueStatistics] object of
 * the column and then combines these [Selectivity] values as if they were uncorrelated.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
object NaiveSelectivityCalculator {
    /**
     * Estimates the selectivity of a [BooleanPredicate] given the [ValueStatistics].
     *
     * @param predicate The [BooleanPredicate] to evaluate.
     * @param statistics The map of [ValueStatistics] to use in the calculation.
     */
    fun estimate(predicate: BooleanPredicate, statistics: Map<ColumnDef<*>,ValueStatistics<*>>): Selectivity = when (predicate) {
        is BooleanPredicate.Atomic -> estimateAtomicReference(predicate, statistics)
        is BooleanPredicate.Compound -> estimateCompoundSelectivity(predicate, statistics)
    }

    /**
     * Estimates the selectivity of a [BooleanPredicate.Atomic] given the [ValueStatistics].
     *
     * @param predicate The [BooleanPredicate.Atomic] to evaluate.
     * @param statistics The map of [ValueStatistics] to use in the calculation.
     */
    private fun estimateAtomicReference(predicate: BooleanPredicate.Atomic, statistics: Map<ColumnDef<*>,ValueStatistics<*>>): Selectivity {
        val left = predicate.operator.left
        if (left !is Binding.Column) {
            return DEFAULT_SELECTIVITY
        }
        val stat = statistics[left.column] ?: return DEFAULT_SELECTIVITY
        return when(val op = predicate.operator) {
            /* Assumption: All elements in IN are matches. */
            is ComparisonOperator.In ->  if (predicate.not) {
                Selectivity((stat.numberOfEntries - op.right.size).toFloat() / stat.numberOfEntries.toFloat())
            } else {
                Selectivity(op.right.size / stat.numberOfEntries.toFloat())
            }

            /* Assumption: Number of NULL / NON-NULL values can be derived directly from statistics. */
            is ComparisonOperator.IsNull -> if (predicate.not) {
                Selectivity(stat.numberOfNonNullEntries.toFloat() / stat.numberOfEntries.toFloat())
            } else {
                Selectivity(stat.numberOfNullEntries.toFloat() / stat.numberOfEntries.toFloat())
            }

            else -> DEFAULT_SELECTIVITY
        }
    }

    /**
     * Estimates the selectivity for a [BooleanPredicate.Compound].
     *
     * This is a very naive approach, which assumes that the [BooleanPredicate]s that make up
     * a [BooleanPredicate.Compound] are independent (i.e. there is no correlation between them).
     *
     * @param predicate The [BooleanPredicate.Compound] to evaluate.
     * @param statistics The map of [ValueStatistics] to use in the calculation.
     */
    private fun estimateCompoundSelectivity(predicate: BooleanPredicate.Compound, statistics: Map<ColumnDef<*>,ValueStatistics<*>>): Selectivity {
        val pp1 = estimate(predicate.p1, statistics)
        val pp2 = estimate(predicate.p2, statistics)
        return when (predicate) {
            is BooleanPredicate.Compound.And ->  pp1 * pp2
            is BooleanPredicate.Compound.Or -> pp1 + pp2 - pp1 * pp2
        }
    }
}