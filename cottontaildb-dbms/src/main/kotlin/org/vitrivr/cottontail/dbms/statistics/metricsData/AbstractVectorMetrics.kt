package org.vitrivr.cottontail.dbms.statistics.metricsData

import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.ComparisonOperator
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.types.RealValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.statistics.selectivity.Selectivity

/**
 * A [ValueMetrics] implementation for all Vector Values.
 *
 * @author Florian Burkhardt
 * @version 1.0.0
 */
sealed class AbstractVectorMetrics<T: Value>(type: Types<T>): AbstractValueMetrics<T>(type) {

    /** A histogram capturing the number of distinct entries per component. */
    val numberOfDistinctEntriesArray: LongArray = LongArray(this.type.logicalSize)

    /**
     * Estimates [Selectivity] of the given [BooleanPredicate.Atomic], i.e., the percentage of [org.vitrivr.cottontail.core.basics.Record]s that match it.
     * Defaults to [Selectivity.DEFAULT] but can be overridden by concrete implementations.
     *
     * @param predicate [BooleanPredicate.Atomic] To estimate [Selectivity] for.
     * @return [Selectivity] estimate.
     */
    context(BindingContext, Record)
    override fun estimateSelectivity(predicate: BooleanPredicate.Atomic): Selectivity = when(val operator = predicate.operator){
        // Default // TODO FOR VECTORS!
        else -> Selectivity.DEFAULT
    }
}