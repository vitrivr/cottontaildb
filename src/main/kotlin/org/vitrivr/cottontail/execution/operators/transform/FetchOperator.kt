package org.vitrivr.cottontail.execution.operators.transform

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.types.Value

/**
 * An [Operator.PipelineOperator] used during query execution. Fetches the specified [ColumnDef] from
 * the specified [Entity]. Can  be used for late population of requested [ColumnDef]s.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class FetchOperator(parent: Operator, val entity: EntityTx, val fetch: List<Pair<Name.ColumnName,ColumnDef<*>>>) : Operator.PipelineOperator(parent) {

    /** Columns returned by [FetchOperator] are a combination of the parent and the [FetchOperator]'s columns */
    override val columns: List<ColumnDef<*>> = this.parent.columns + this.fetch.map { it.second.copy(name = it.first) }

    /** [FetchOperator] does not act as a pipeline breaker. */
    override val breaker: Boolean = false

    /**
     * Converts this [FetchOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [FetchOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val fetch = this.fetch.map { it.second }.toTypedArray()
        val columns = this.columns.toTypedArray()
        val values = arrayOfNulls<Value?>(this.columns.size)
        return this.parent.toFlow(context).map { r ->
            var i = 0
            r.forEach { _, v -> values[i++] = v }
            this@FetchOperator.entity.read(r.tupleId, fetch).forEach { _, v -> values[i++] = v }
            val record = StandaloneRecord(r.tupleId, columns, values)
            this@FetchOperator.binding.bindRecord(record) /* Important: Make new record available to binding context. */
            record
        }
    }
}