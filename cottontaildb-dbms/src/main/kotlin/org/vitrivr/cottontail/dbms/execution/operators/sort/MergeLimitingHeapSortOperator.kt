package org.vitrivr.cottontail.dbms.execution.operators.sort

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flattenMerge
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.dbms.execution.TransactionContext
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.sort.SortOrder
import org.vitrivr.cottontail.utilities.selection.HeapSelection

/**
 * A [MergeLimitingHeapSortOperator] merges the results of multiple incoming operators into a single [Flow],
 * orders them by a list of specified [ColumnDef] and limits the number of output [Record]s.
 *
 * This is often used in parallelized proximity based queries.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class MergeLimitingHeapSortOperator(parents: List<Operator>, val context: BindingContext, sortOn: List<Pair<ColumnDef<*>, SortOrder>>, val limit: Long) : Operator.MergingPipelineOperator(parents) {

    /** The columns produced by this [MergeLimitingHeapSortOperator]. */
    override val columns: List<ColumnDef<*>> = this.parents.first().columns

    /** The [HeapSortOperator] is always a pipeline breaker. */
    override val breaker: Boolean = true

    /** The [Comparator] used for sorting. */
    private val comparator: Comparator<Record> = RecordComparator.fromList(sortOn)

    /**
     * Converts this [MergeLimitingHeapSortOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [MergeLimitingHeapSortOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> = flow {
        /* Collect incoming flows. */
        val parents = this@MergeLimitingHeapSortOperator.parents.map { it.toFlow(context) }.toTypedArray()
        val selection = HeapSelection(this@MergeLimitingHeapSortOperator.limit, this@MergeLimitingHeapSortOperator.comparator)
        flowOf(*parents).flattenMerge(parents.size).collect {
            selection.offer(it)
        }

        /* Emit sorted and limited values. */
        for (i in 0 until selection.size) {
            val rec = selection[i]
            this@MergeLimitingHeapSortOperator.context.update(rec)
            emit(rec)
        }
    }
}