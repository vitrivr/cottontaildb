package org.vitrivr.cottontail.dbms.execution.operators.sort

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.merge
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.utilities.selection.HeapSelection

/**
 * A [MergeLimitingHeapSortOperator] merges the results of multiple incoming operators into a single [Flow],
 * orders them by a list of specified [ColumnDef] and limits the number of output [Tuple]s.
 *
 * This is often used in parallelized proximity based queries.
 *
 * @author Ralph Gasser
 * @version 2.2.0
 */
class MergeLimitingHeapSortOperator(parents: List<Operator>, sortOn: List<Pair<Binding.Column, SortOrder>>, private val limit: Int, override val context: QueryContext) : Operator.MergingPipelineOperator(parents) {

    companion object {
        /** [Logger] instance used by [MergeLimitingHeapSortOperator]. */
        private val LOGGER: Logger = LoggerFactory.getLogger(MergeLimitingHeapSortOperator::class.java)
    }

    /** The columns produced by this [MergeLimitingHeapSortOperator]. */
    override val columns: List<ColumnDef<*>> = this.parents.first().columns

    /** The [HeapSortOperator] is always a pipeline breaker. */
    override val breaker: Boolean = true

    /** The [Comparator] used for sorting. */
    private val comparator: Comparator<Tuple> = RecordComparator.fromList(sortOn.map { it.first.column to it.second })

    /**
     * Converts this [MergeLimitingHeapSortOperator] to a [Flow] and returns it.
     *
     * @return [Flow] representing this [MergeLimitingHeapSortOperator]
     */
    override fun toFlow(): Flow<Tuple> = flow {
        val globalSelection = HeapSelection(this@MergeLimitingHeapSortOperator.limit, this@MergeLimitingHeapSortOperator.comparator)
        this@MergeLimitingHeapSortOperator.parents.map { it.toFlow() }.merge().collect {
            globalSelection.offer(it)
        }

        /* Emit final result .*/
        for (r in globalSelection) {
            emit(r)
        }
    }
}