package org.vitrivr.cottontail.dbms.execution.operators.transform

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * An [Operator.PipelineOperator] used during query execution. Fetches the specified [ColumnDef] from
 * the specified [Entity]. Can  be used for late population of requested [ColumnDef]s.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class FetchOperator(parent: Operator, private val entity: EntityTx, private val fetch: List<Pair<Binding.Column, ColumnDef<*>>>, override val context: QueryContext) : Operator.PipelineOperator(parent) {

    /** Columns returned by [FetchOperator] are a combination of the parent and the [FetchOperator]'s columns */
    override val columns: List<ColumnDef<*>> = this.parent.columns + this.fetch.map { it.first.column }

    /** [FetchOperator] does not act as a pipeline breaker. */
    override val breaker: Boolean = false

    /**
     * Converts this [FetchOperator] to a [Flow] and returns it.
     *
     * @return [Flow] representing this [FetchOperator]
     */
    override fun toFlow(): Flow<Record> = flow {
        val fetch = this@FetchOperator.fetch.map { it.second }.toTypedArray()
        val columns = this@FetchOperator.columns.toTypedArray()
        val incoming = this@FetchOperator.parent.toFlow()
        val cursors = fetch.map {
            this@FetchOperator.entity.columnForName(it.name).newTx(this@FetchOperator.context).cursor()
        }

        val numberOfInputColumns = this@FetchOperator.parent.columns.size
        try  {
            incoming.collect { record ->
                val values = Array(this@FetchOperator.columns.size) {
                    if (it < numberOfInputColumns) {
                        record[it]
                    } else {
                        val cursor = cursors[it-numberOfInputColumns]
                        require(cursor.moveTo(record.tupleId)) {
                            "TupleId ${record.tupleId} could not be fetched! This is a programmer's error!"
                        }
                        cursor.value()
                    }
                }
                emit(StandaloneRecord(record.tupleId, columns, values))
            }
        } finally {
            cursors.forEach { it.close() }
        }
    }
}