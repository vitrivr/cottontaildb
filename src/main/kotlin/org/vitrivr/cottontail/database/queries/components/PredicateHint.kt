package org.vitrivr.cottontail.database.queries.predicates

import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.model.basics.Name

/**
 * A hint for a [Predicate] that can be used by the query optimizer to make appropriate decisions.
 *
 * The query optimizer will make the best effort honor the hint specified in a [PredicateHint].
 * However,  it is at the discretion of the optimiser and the execution engine, whether a given
 * hint can and will be honored.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
sealed class PredicateHint

/**
 * A [PredicateHint] that only affects [KnnPredicate]s. These types of hints can help the query planner
 * to make an informed decision regarding the choice of [Index]
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
sealed class KnnPredicateHint : PredicateHint() {

    /**
     * Checks if the provided [Index] satisfies this [KnnPredicateHint].
     *
     * @param index The [Index] to check.
     * @return True, if the [Index] satisfies this [KnnPredicateHint]
     */
    abstract fun satisfies(index: Index): Boolean

    /**
     * A [PredicateHint] that indicates, that only exact [Index]es are allowed
     */
    object KnnExactPredicateHint : KnnPredicateHint() {
        override fun satisfies(index: Index): Boolean = !index.type.inexact
    }

    /**
     * A [PredicateHint] that indicates, that usage of inexact [Index]es are allowed.
     */
    object KnnInexactPredicateHint : KnnPredicateHint() {
        override fun satisfies(index: Index): Boolean = (index.type.inexact || !index.type.inexact)
    }

    /**
     * A [PredicateHint] that indicates, that no [Index] should be used.
     */
    object KnnNoIndexPredicateHint : KnnPredicateHint() {
        override fun satisfies(index: Index): Boolean = false
    }

    /**
     * A [PredicateHint] that indicates, what [IndexType] should be used.
     */
    data class KnnIndexTypePredicateHint(val type: IndexType) : KnnPredicateHint() {
        override fun satisfies(index: Index): Boolean = index.type == type
    }

    /**
     * A [PredicateHint] that indicates, what [Index] should be used (identified by name).
     */
    data class KnnIndexNamePredicateHint(val name: Name.IndexName) : KnnPredicateHint() {
        override fun satisfies(index: Index): Boolean = index.name == name
    }

    /**
     * A [PredicateHint] that indicates, how many threads should be used for execution.
     */
    data class KnnParallelismPredicateHint(val min: Int, val max: Int) : KnnPredicateHint() {
        override fun satisfies(index: Index): Boolean = true
    }
}