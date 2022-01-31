package org.vitrivr.cottontail.dbms.execution.operators.sort

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.dbms.execution.TransactionContext
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.sort.SortOrder
import org.vitrivr.cottontail.utilities.selection.HeapSelection

/**
 * An [Operator.PipelineOperator] used during query execution. Performs sorting on the specified
 * [ColumnDef]s and returns the [Record] in sorted order. Limits the number of [Record]s produced.
 *
 * Acts as pipeline breaker.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class LimitingHeapSortOperator(parent: Operator, sortOn: List<Pair<ColumnDef<*>, SortOrder>>, private val limit: Long, private val skip: Long) : AbstractSortOperator(parent, sortOn) {

    /** The [HeapSortOperator] retains the [ColumnDef] of the input. */
    override val columns: List<ColumnDef<*>>
        get() = this.parent.columns

    /** The [HeapSortOperator] is always a pipeline breaker. */
    override val breaker: Boolean = true

    /**
     * Converts this [LimitingHeapSortOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [LimitingHeapSortOperator]
     */
    override fun toFlow(context: org.vitrivr.cottontail.dbms.execution.TransactionContext): Flow<Record> {
        val parentFlow = this.parent.toFlow(context)
        return flow {
            val selection = HeapSelection(this@LimitingHeapSortOperator.limit + this@LimitingHeapSortOperator.skip, this@LimitingHeapSortOperator.comparator)
            parentFlow.collect { selection.offer(it.copy()) } /* Important: Materialization! */
            for (i in this@LimitingHeapSortOperator.skip until selection.size) {
                emit(selection[i])
            }
        }
    }
}