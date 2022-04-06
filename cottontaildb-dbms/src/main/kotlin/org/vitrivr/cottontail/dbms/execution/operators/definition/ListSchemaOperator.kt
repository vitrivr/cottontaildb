package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.queries.operators.ColumnSets
import org.vitrivr.cottontail.dbms.schema.Schema

/**
 * An [Operator.SourceOperator] used during query execution. Lists all available [Schema]s.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class ListSchemaOperator(private val tx: CatalogueTx) : Operator.SourceOperator() {
    override val columns: List<ColumnDef<*>> = ColumnSets.DDL_LIST_COLUMNS
    override fun toFlow(context: TransactionContext): Flow<Record> = flow {
        val columns = this@ListSchemaOperator.columns.toTypedArray()
        for ((i, schema) in this@ListSchemaOperator.tx.listSchemas().withIndex()) {
            emit(StandaloneRecord(i + 1L, columns, arrayOf(StringValue(schema.fqn), StringValue("SCHEMA"))))
        }
    }
}