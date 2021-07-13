package org.vitrivr.cottontail.execution.operators.sources

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.queries.GroupId
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.types.Value

/**
 * An [AbstractEntityOperator] that scans an [Entity] and streams all [Record]s found within.
 *
 * @author Ralph Gasser
 * @version 1.5.0
 */
class EntityScanOperator(groupId: GroupId, entity: EntityTx, fetch: Map<Name.ColumnName,ColumnDef<*>>, private val partitionIndex: Int, private val partitions: Int) : AbstractEntityOperator(groupId, entity, fetch) {
    /**
     * Converts this [EntityScanOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [EntityScanOperator]
     */
    override fun toFlow(context: QueryContext): Flow<Record> {
        val fetch = this.fetch.values.toTypedArray()
        val columns = this.columns.toTypedArray()
        val values = arrayOfNulls<Value?>(this.columns.size)
        return flow {
            for (record in this@EntityScanOperator.entity.scan(fetch, this@EntityScanOperator.partitionIndex, this@EntityScanOperator.partitions)) {
                var i = 0
                record.forEach { _, v -> values[i++] = v }
                val r = StandaloneRecord(record.tupleId, columns, values)
                context.bindings.bindRecord(r) /* Important: Make new record available to binding context. */
                emit(r)
            }
        }
    }
}