package org.vitrivr.cottontail.dbms.execution.operators.sources

import it.unimi.dsi.fastutil.longs.LongOpenHashSet
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.launch
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.index.basic.IndexTx
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * The [IndexIntersectionScanOperator] is a [Operator.SourceOperator] that is used to intersect the results of multiple [IndexTx] scans.
 *
 * [IndexIntersectionScanOperator] can only be used in specific scenarios:
 * - All the [Predicate]s must be connected by an AND operator.
 * - All the [IndexTx]s must be on the same [Entity].
 * - All the [IndexTx] only return the columns of that [Entity].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class IndexIntersectionScanOperator(groupId: GroupId, private val indexes: List<Pair<IndexTx, Predicate>>, override val context: QueryContext): Operator.SourceOperator(groupId) {

    init {
        require(indexes.asSequence().map { it.first.dbo.parent }.distinct().count() == 1) {
            "All IndexTx for an INDEX INTERSECTION SCAN must be on the same entity. This is a programmer's error!"
        }
    }

    override val columns: List<ColumnDef<*>> = this.indexes.first().first.parent.listColumns()

    /**
     * Converts this [IndexIntersectionScanOperator] to a [Flow] and returns it.
     *
     * @return [Flow] representing this [IndexIntersectionScanOperator]
     */
    override fun toFlow(): Flow<Tuple> = channelFlow {
        /* Prepare jobs to scan indexes. */
        val jobs = this@IndexIntersectionScanOperator.indexes.map { (index, predicate) ->
            val bitSet = LongOpenHashSet()
            launch {
                index.filter(predicate).use { cursor ->
                    while (cursor.moveNext()) {
                        val tupleId = cursor.key()
                        bitSet.add(tupleId)
                    }
                }
            } to bitSet
        }

        /* Wait for all jobs to finish. */
        jobs.forEach { it.first.join() }

        /* Intersect all bitsets. */
        val intersection = jobs.first().second
        for (i in 1 until jobs.size) {
            intersection.retainAll(jobs[i].second)
        }

        /* Emit results. */
        for (tupleId in intersection.longIterator()) {
            send(this@IndexIntersectionScanOperator.indexes.first().first.parent.read(tupleId))
        }
    }
}