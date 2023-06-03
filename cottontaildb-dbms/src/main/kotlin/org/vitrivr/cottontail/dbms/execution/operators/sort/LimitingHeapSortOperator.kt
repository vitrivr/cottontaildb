package org.vitrivr.cottontail.dbms.execution.operators.sort

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.utilities.selection.HeapSelection

/**
 * An [Operator.PipelineOperator] used during query execution. Performs sorting on the specified
 * [ColumnDef]s and returns the [Tuple] in sorted order. Limits the number of [Tuple]s produced.
 *
 * Acts as pipeline breaker.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class LimitingHeapSortOperator(parent: Operator, sortOn: List<Pair<ColumnDef<*>, SortOrder>>, private val limit: Long, override val context: QueryContext) : AbstractSortOperator(parent, sortOn) {

    /** The [HeapSortOperator] retains the [ColumnDef] of the input. */
    override val columns: List<ColumnDef<*>>
        get() = this.parent.columns

    /** The [HeapSortOperator] is always a pipeline breaker. */
    override val breaker: Boolean = true

    /**
     * Converts this [LimitingHeapSortOperator] to a [Flow] and returns it.
     *
     * @return [Flow] representing this [LimitingHeapSortOperator]
     */
    override fun toFlow(): Flow<Tuple> = flow {
        val incoming = this@LimitingHeapSortOperator.parent.toFlow()
        val selection = HeapSelection(this@LimitingHeapSortOperator.limit.toInt(), this@LimitingHeapSortOperator.comparator)
        incoming.collect { selection.offer(it) }
        for(r in selection) emit(r)
    }
}