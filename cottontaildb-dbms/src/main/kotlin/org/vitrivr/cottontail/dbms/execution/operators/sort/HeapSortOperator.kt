package org.vitrivr.cottontail.dbms.execution.operators.sort

import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.dbms.execution.TransactionContext
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.sort.SortOrder

/**
 * An [Operator.PipelineOperator] used during query execution. Performs sorting on the specified [ColumnDef]s and
 * returns the [Record] in sorted order. Acts as pipeline breaker.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
open class HeapSortOperator(parent: Operator, sortOn: List<Pair<ColumnDef<*>, SortOrder>>, private val queueSize: Int) : AbstractSortOperator(parent, sortOn) {
    /**
     * Converts this [HeapSortOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [HeapSortOperator]
     */
    override fun toFlow(context: org.vitrivr.cottontail.dbms.execution.TransactionContext): Flow<Record> {
        val parentFlow = this.parent.toFlow(context)
        return flow {
            val queue = ObjectHeapPriorityQueue(this@HeapSortOperator.queueSize, this@HeapSortOperator.comparator)
            parentFlow.collect { queue.enqueue(it.copy()) } /* Important: Materialization! */
            while (!queue.isEmpty) {
                emit(queue.dequeue())
            }
        }
    }
}