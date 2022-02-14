package org.vitrivr.cottontail.dbms.execution.operators.sources

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.dbms.execution.TransactionContext
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.index.Index
import org.vitrivr.cottontail.dbms.index.IndexTx

/**
 * An [Operator.SourceOperator] that scans an [Index] and streams all [Record]s found within.
 *
 * @author Ralph Gasser
 * @version 1.6.0
 */
class IndexScanOperator(
    groupId: GroupId,
    private val index: IndexTx,
    private val predicate: Predicate,
    private val fetch: List<Pair<Binding.Column, ColumnDef<*>>>,
    private val partitionIndex: Int = 0,
    private val partitions: Int = 1
) : Operator.SourceOperator(groupId) {

    /** The [ColumnDef] produced by this [IndexScanOperator]. */
    override val columns: List<ColumnDef<*>> = this.fetch.map {
        require(this.index.dbo.produces.contains(it.second)) { "The given column $it is not produced by the selected index ${this.index.dbo}. This is a programmer's error!"}
        it.first.column
    }

    /**
     * Converts this [IndexScanOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution.
     * @return [Flow] representing this [IndexScanOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val columns = this.fetch.map { it.first.column }.toTypedArray()
        return flow {
            val iterator = if (this@IndexScanOperator.partitions == 1) {
                this@IndexScanOperator.index.filter(this@IndexScanOperator.predicate)
            } else {
                this@IndexScanOperator.index.filterRange(this@IndexScanOperator.predicate, this@IndexScanOperator.partitionIndex, this@IndexScanOperator.partitions)
            }
            for (record in iterator) {
                val rec = StandaloneRecord(record.tupleId, columns, Array(fetch.size) { record[it] })
                this@IndexScanOperator.fetch.first().first.context.update(rec)
                emit(rec)
            }
        }
    }
}