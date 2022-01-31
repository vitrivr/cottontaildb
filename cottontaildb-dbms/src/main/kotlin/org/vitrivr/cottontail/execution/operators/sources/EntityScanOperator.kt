package org.vitrivr.cottontail.execution.operators.sources

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator

/**
 * An [Operator.SourceOperator] that scans an [Entity] and streams all [Record]s found within.
 *
 * @author Ralph Gasser
 * @version 1.6.0
 */
class EntityScanOperator(groupId: GroupId, val entity: EntityTx, val fetch: List<Pair<Binding.Column, ColumnDef<*>>>, val partitionIndex: Int, val partitions: Int) : Operator.SourceOperator(groupId) {

    /** The [ColumnDef] fetched by this [EntityScanOperator]. */
    override val columns: List<ColumnDef<*>> = this.fetch.map { it.first.column }

    /**
     * Converts this [EntityScanOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [EntityScanOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val fetch = this.fetch.map { it.second }.toTypedArray()
        val columns = this.fetch.map { it.first.column }.toTypedArray()
        return flow {
            for (record in this@EntityScanOperator.entity.scan(fetch, this@EntityScanOperator.partitionIndex, this@EntityScanOperator.partitions)) {
                for (i in 0 until record.size) {
                    record.columns[i] = columns[i] /* Rename columns. */
                    this@EntityScanOperator.fetch[i].first.update(record[i])
                }
                emit(record)
            }
        }
    }
}