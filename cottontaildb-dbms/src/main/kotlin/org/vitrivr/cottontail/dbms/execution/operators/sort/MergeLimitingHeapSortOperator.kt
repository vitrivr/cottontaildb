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
 * @version 1.4.0
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
    override fun toFlow(context: TransactionContext): Flow<Record> = channelFlow {

        /* Prepare a global heap selection; this selection is large enough to accept [limit] entries from each partition to prevent concurrent sorting. */
        val incoming = this@MergeLimitingHeapSortOperator.parents
        val globalSelection = HeapSelection(
            this@MergeLimitingHeapSortOperator.limit * incoming.size,
            this@MergeLimitingHeapSortOperator.comparator
        )
        val globalCollected = AtomicLong(0L)

        /*
         * Collect incoming flows into a local HeapSelection object (one per flow to avoid contention).
         *
         * For pre-sorted and pre-limited input, the HeapSelection should incur only minimal overhead because
         * sorting only kicks in if k entries have been added.
         */
        val jobs = incoming.map { op ->
            launch {
                val localSelection = HeapSelection(this@MergeLimitingHeapSortOperator.limit, this@MergeLimitingHeapSortOperator.comparator)
                var localCollected = 0L
                op.toFlow(context).collect {
                    localCollected += 1L
                    localSelection.offer(it)
                }
                for (i in 0 until localSelection.size) {
                    globalSelection.offer(localSelection[i])
                }
                globalCollected.addAndGet(localCollected)
            }
        }
        jobs.forEach { it.join() } /* Wait for jobs to complete. */
        LOGGER.debug("Collection of ${globalCollected.get()} records from ${jobs.size} partitions completed! ")

        for (i in 0 until this@MergeLimitingHeapSortOperator.limit) {
            val rec = globalSelection[i]
            this@MergeLimitingHeapSortOperator.context.update(rec)
            send(rec)
        }
    }
}