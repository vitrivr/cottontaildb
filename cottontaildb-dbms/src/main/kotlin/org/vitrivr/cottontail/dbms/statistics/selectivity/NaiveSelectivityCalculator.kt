package org.vitrivr.cottontail.dbms.statistics.selectivity

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.statistics.values.ValueStatistics

/**
 * This is a very naive calculator for [Selectivity] values.
 *
 * It simply delegates [Selectivity] calculation for [BooleanPredicate.Comparison]to the [ValueStatistics] object of
 * the column and then combines these [Selectivity] values as if they were uncorrelated.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
object NaiveSelectivityCalculator {

    /**
     * Estimates the selectivity of a [BooleanPredicate] given the [ValueStatistics].
     *
     * @param predicate The [BooleanPredicate] to evaluate.
     * @param statistics The map of [ValueStatistics] to use in the calculation.
     */
    context(BindingContext, Tuple)
    fun estimate(predicate: BooleanPredicate, statistics: Map<ColumnDef<*>, ValueStatistics<*>>): Selectivity = when (predicate) {
        is BooleanPredicate.Literal -> if (predicate.boolean) Selectivity.ALL else Selectivity.NOTHING
        is BooleanPredicate.IsNull -> this.estimateIsNull(predicate, statistics)
        is BooleanPredicate.Comparison -> this.estimateComparison(predicate, statistics)
        is BooleanPredicate.Not -> Selectivity(1.0f - this.estimate(predicate.p, statistics).value)
        is BooleanPredicate.And -> this.estimate(predicate.p1, statistics) * this.estimate(predicate.p2, statistics)
        is BooleanPredicate.Or -> {
            val pp1 = this.estimate(predicate.p1, statistics)
            val pp2 = this.estimate(predicate.p2, statistics)
            pp1 + pp2 - pp1 * pp2
        }
    }

    /**
     * Estimates the selectivity of a [BooleanPredicate.IsNull] given the [ValueStatistics].
     *
     * @param predicate The [BooleanPredicate.IsNull] to evaluate.
     * @param statistics The map of [ValueStatistics] to use in the calculation.
     */
    context(BindingContext,Tuple)
    private fun estimateIsNull(predicate: BooleanPredicate.IsNull, statistics: Map<ColumnDef<*>, ValueStatistics<*>>): Selectivity {
        return when(val binding = predicate.binding) {
            is Binding.Column ->  statistics[binding.column]?.let { Selectivity(it.numberOfNullEntries.toFloat() / it.numberOfEntries) } ?: Selectivity.DEFAULT
            is Binding.Literal -> if (binding.getValue() == null) Selectivity.ALL else Selectivity.NOTHING
            else -> Selectivity.DEFAULT
        }
    }

    /**
     * Estimates the selectivity of a [BooleanPredicate.Comparison] given the map of [ValueStatistics].
     *
     * There is a few cases we can actually handle:
     * - [Binding.Literal] to [Binding.Literal] comparison
     * - [Binding.Column] to [Binding.Literal] comparison
     *
     * @param predicate The [BooleanPredicate.Comparison] to evaluate.
     * @param statistics The map of [ValueStatistics] to use in the calculation.
     */
    context(BindingContext,Tuple)
    private fun estimateComparison(predicate: BooleanPredicate.Comparison, statistics: Map<ColumnDef<*>, ValueStatistics<*>>): Selectivity {
        val left = predicate.operator.left
        val right = predicate.operator.right
        return when {
            left is Binding.Literal && right is Binding.Literal -> if (left.getValue() == right.getValue()) Selectivity.ALL else Selectivity.NOTHING
            left !is Binding.Column && right is Binding.Column -> statistics[right.physical]?.estimateSelectivity(predicate) ?: Selectivity.DEFAULT
            left is Binding.Column && right !is Binding.Column -> statistics[left.physical]?.estimateSelectivity(predicate) ?: Selectivity.DEFAULT
            else -> Selectivity.DEFAULT
        }
    }
}