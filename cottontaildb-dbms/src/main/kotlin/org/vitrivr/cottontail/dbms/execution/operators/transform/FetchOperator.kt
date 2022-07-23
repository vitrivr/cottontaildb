package org.vitrivr.cottontail.dbms.execution.operators.transform

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onCompletion
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.dbms.column.ColumnTx
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext

/**
 * An [Operator.PipelineOperator] used during query execution. Fetches the specified [ColumnDef] from
 * the specified [Entity]. Can  be used for late population of requested [ColumnDef]s.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class FetchOperator(parent: Operator, val entity: EntityTx, val fetch: List<Pair<Binding.Column, ColumnDef<*>>>) : Operator.PipelineOperator(parent) {

    /** Columns returned by [FetchOperator] are a combination of the parent and the [FetchOperator]'s columns */
    override val columns: List<ColumnDef<*>> = this.parent.columns + this.fetch.map { it.first.column }

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
        val cursors = fetch.map {
            (context.getTx(this@FetchOperator.entity.columnForName(it.name)) as ColumnTx<*>).cursor()
        }
        val numberOfInputColumns = this.parent.columns.size
        val bindingContext = this@FetchOperator.fetch.first().first.context
        return this.parent.toFlow(context).map { r ->
            val values = Array(this.columns.size) {
                if (it < numberOfInputColumns) {
                    r[it]
                } else {
                    val cursor = cursors[it-numberOfInputColumns]
                    require(cursor.moveTo(r.tupleId)) { "TupleId ${r.tupleId} could not be obtained via cr"}
                    cursor.value()
                }
            }
            val rec = StandaloneRecord(r.tupleId, columns, values) /* New record is emitted. */
            bindingContext.update(rec)
            rec
        }.onCompletion {
            cursors.forEach { it.close() }
        }
    }
}