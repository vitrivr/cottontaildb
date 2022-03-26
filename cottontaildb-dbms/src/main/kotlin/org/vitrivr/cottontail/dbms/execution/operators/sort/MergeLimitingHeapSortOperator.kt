package org.vitrivr.cottontail.dbms.execution.operators.sort

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.utilities.selection.HeapSelection
import java.util.concurrent.atomic.AtomicLong

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

    companion object {
        /** [Logger] instance used by [MergeLimitingHeapSortOperator]. */
        private val LOGGER: Logger = LoggerFactory.getLogger(MergeLimitingHeapSortOperator::class.java)
    }

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
    override fun toFlow(context: TransactionContext): Flow<Record> =  channelFlow {
        /* Collect incoming flows into dedicated HeapSelection objects (one per flow). */
        val selection = HeapSelection(this@MergeLimitingHeapSortOperator.limit, this@MergeLimitingHeapSortOperator.comparator)
        val collected = AtomicLong(0L)
        val jobs = this@MergeLimitingHeapSortOperator.parents.map { p ->
            launch {
                p.toFlow(context).collect {
                    collected.incrementAndGet()
                    selection.offer(it)
                }
            }
        }
        jobs.forEach { it.join() } /* Wait for jobs to complete. */
        LOGGER.debug("Collection of ${collected.get()} records from ${jobs.size} partitions completed! ")

        /* Emit sorted and limited values. */
        for (i in 0 until selection.size) {
            val rec = selection[i]
            this@MergeLimitingHeapSortOperator.context.update(rec)
            send(rec)
        }
    }
}