package org.vitrivr.cottontail.execution.operators.sources

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.queries.GroupId
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.types.Value

/**
 * An [Operator.SourceOperator] that scans an [Entity] and streams all [Record]s found within.
 *
 * @author Ralph Gasser
 * @version 1.6.0
 */
class EntityScanOperator(groupId: GroupId, val entity: EntityTx, val fetch: List<Pair<Name.ColumnName,ColumnDef<*>>>, override val binding: BindingContext, val partitionIndex: Int, val partitions: Int) : Operator.SourceOperator(groupId) {

    /** The [ColumnDef] fetched by this [EntityScanOperator]. */
    override val columns: List<ColumnDef<*>> = this.fetch.map { it.second.copy(name = it.first) }

    /**
     * Converts this [EntityScanOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [EntityScanOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val fetch = this.fetch.map { it.second }.toTypedArray()
        val columns = this.columns.toTypedArray()
        val values = arrayOfNulls<Value?>(this.columns.size)
        return flow {
            for (record in this@EntityScanOperator.entity.scan(fetch, this@EntityScanOperator.partitionIndex, this@EntityScanOperator.partitions)) {
                var i = 0
                record.forEach { _, v -> values[i++] = v }
                val r = StandaloneRecord(record.tupleId, columns, values)
                this@EntityScanOperator.binding.bindRecord(r) /* Important: Make new record available to binding context. */
                emit(r)
            }
        }
    }
}