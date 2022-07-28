package org.vitrivr.cottontail.dbms.execution.operators.sort

import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * An [Operator.PipelineOperator] used during query execution. Performs sorting on the specified [ColumnDef]s and
 * returns the [Record] in sorted order. Acts as pipeline breaker.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
open class HeapSortOperator(parent: Operator, sortOn: List<Pair<ColumnDef<*>, SortOrder>>, private val queueSize: Int, override val context: QueryContext) : AbstractSortOperator(parent, sortOn) {
    /**
     * Converts this [HeapSortOperator] to a [Flow] and returns it.
     *
     * @return [Flow] representing this [HeapSortOperator]
     */
    override fun toFlow(): Flow<Record> = flow {
        val incoming = this@HeapSortOperator.parent.toFlow()
        val queue = ObjectHeapPriorityQueue(this@HeapSortOperator.queueSize, this@HeapSortOperator.comparator)
        incoming.collect { queue.enqueue(it) }
        while (!queue.isEmpty) {
            emit(queue.dequeue())
        }
    }
}