package org.vitrivr.cottontail.execution.operators.sources

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.queries.GroupId
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record

/**
 * An [AbstractEntityOperator] that scans an [Index] and streams all [Record]s found within.
 *
 * @author Ralph Gasser
 * @version 1.6.0
 */
class IndexScanOperator(
    groupId: GroupId,
    private val index: IndexTx,
    private val predicate: Predicate,
    private val fetch: List<Pair<Name.ColumnName,ColumnDef<*>>>,
    override val binding: BindingContext,
    private val partitionIndex: Int = 0,
    private val partitions: Int = 1
) : Operator.SourceOperator(groupId) {

    /** The [ColumnDef] produced by this [IndexScanOperator]. */
    override val columns: List<ColumnDef<*>> = this.fetch.map {
        require(this.index.dbo.produces.contains(it.second)) { "The given column $it is not produced by the selected index ${this.index.dbo}. This is a programmer's error!"}
        it.second.copy(name = it.first)
    }

    /**
     * Converts this [IndexScanOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution.
     * @return [Flow] representing this [IndexScanOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        return flow {
            if (this@IndexScanOperator.partitions == 1) {
                this@IndexScanOperator.index.filter(this@IndexScanOperator.predicate).forEach {
                    this@IndexScanOperator.binding.bindRecord(it) /* Important: Make new record available to binding context. */
                    emit(it)
                }
            } else {
                this@IndexScanOperator.index.filterRange(this@IndexScanOperator.predicate, this@IndexScanOperator.partitionIndex, this@IndexScanOperator.partitions).forEach {
                    this@IndexScanOperator.binding.bindRecord(it) /* Important: Make new record available to binding context. */
                    emit(it)
                }
            }
        }
    }
}