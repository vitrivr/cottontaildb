package org.vitrivr.cottontail.dbms.execution.operators.transform

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.onCompletion
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * An [Operator.PipelineOperator] used during query execution. Fetches the specified [ColumnDef] from
 * the specified [Entity]. Can  be used for late population of requested [ColumnDef]s.
 *
 * @author Ralph Gasser
 * @version 2.2.0
 */
class FetchOperator(parent: Operator, private val tx: EntityTx, private val fetch: List<Binding.Column>, override val context: QueryContext) : Operator.PipelineOperator(parent) {

    /** Columns returned by [FetchOperator] are a combination of the parent and the [FetchOperator]'s columns */
    override val columns: List<ColumnDef<*>> = this.parent.columns + this.fetch.map { it.column }

    /** [FetchOperator] does not act as a pipeline breaker. */
    override val breaker: Boolean = false

    /**
     * Converts this [FetchOperator] to a [Flow] and returns it.
     *
     * @return [Flow] representing this [FetchOperator]
     */
    override fun toFlow(): Flow<Tuple> {
        val fetch = this@FetchOperator.fetch.map { it.physical!! }.toTypedArray()
        val columns = this@FetchOperator.columns.toTypedArray()
        val incoming = this@FetchOperator.parent.toFlow()
        val numberOfInputColumns = this@FetchOperator.parent.columns.size
        val cursors = fetch.map {
            this@FetchOperator.tx.columnForName(it.name).newTx(this@FetchOperator.tx).cursor()
        }

        /* Return mapping flow. */
        return incoming.map { record ->
            val values = Array(this@FetchOperator.columns.size) {
                if (it < numberOfInputColumns) {
                    record[it]
                } else {
                    val cursor = cursors[it-numberOfInputColumns]
                    if (cursor.moveTo(record.tupleId)) {
                        cursor.value()
                    } else {
                        null
                    }
                }
            }
            StandaloneTuple(record.tupleId, columns, values)
        }.onCompletion {
            cursors.forEach { it.close() }
        }
    }
}