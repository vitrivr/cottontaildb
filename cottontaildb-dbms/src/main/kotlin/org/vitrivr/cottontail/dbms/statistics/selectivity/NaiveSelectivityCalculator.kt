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
 * @version 1.1.0
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
        is BooleanPredicate.IsNull -> TODO()
        is BooleanPredicate.Comparison -> this.estimateAtomicReference(predicate, statistics)
        is BooleanPredicate.Not -> this.estimate(predicate.p, statistics)
        is BooleanPredicate.And -> this.estimate(predicate.p1, statistics) * this.estimate(predicate.p2, statistics)
        is BooleanPredicate.Or -> {
            val pp1 = this.estimate(predicate.p1, statistics)
            val pp2 = this.estimate(predicate.p2, statistics)
            pp1 + pp2 - pp1 * pp2
        }
    }

    /**
     * Estimates the selectivity of a [BooleanPredicate.Comparison] given the [ValueStatistics].
     *
     * @param predicate The [BooleanPredicate.Comparison] to evaluate.
     * @param statistics The map of [ValueStatistics] to use in the calculation.
     */
    context(BindingContext, Tuple)
    private fun estimateAtomicReference(predicate: BooleanPredicate.Comparison, statistics: Map<ColumnDef<*>, ValueStatistics<*>>): Selectivity {
        val left = predicate.operator.left
        return if (left is Binding.Column) {
            val stat = statistics[left.column] ?: return Selectivity.DEFAULT
            stat.estimateSelectivity(predicate)
        } else {
            Selectivity.DEFAULT /* TODO: Selectivity estimation based on literals and functions. */
        }
    }
}