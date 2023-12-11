package org.vitrivr.cottontail.dbms.execution.operators.sort

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.utilities.selection.HeapSelection
import java.util.concurrent.atomic.AtomicLong

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
    override fun toFlow(): Flow<Tuple> = channelFlow {

        /* Prepare a global heap selection. */
        val incoming = this@MergeLimitingHeapSortOperator.parents
        val globalSelection = HeapSelection((this@MergeLimitingHeapSortOperator.limit), this@MergeLimitingHeapSortOperator.comparator)
        val globalCollected = AtomicLong(0L)

        /*
         * Collect incoming flows into a local HeapSelection object (one per flow to avoid contention).
         *
         * For pre-sorted and pre-limited input, the HeapSelection should incur only minimal overhead because
         * sorting only kicks in if k entries have been added.
         */
        val mutex = Mutex()
        val jobs = incoming.map { op ->
            launch {
                val localSelection = HeapSelection(this@MergeLimitingHeapSortOperator.limit.toInt(), this@MergeLimitingHeapSortOperator.comparator)
                var localCollected = 0L
                op.toFlow().collect {
                    localCollected += 1L
                    localSelection.offer(it)
                }
                mutex.withLock {
                    for (r in localSelection) {
                       globalSelection.offer(r)
                   }
                }
                globalCollected.addAndGet(localCollected)
            }
        }
        jobs.forEach { it.join() } /* Wait for jobs to complete. */
        LOGGER.debug("Collection of ${globalCollected.get()} records from ${jobs.size} partitions completed! ")
        for (r in globalSelection) {
            send(r)
        }
    }
}