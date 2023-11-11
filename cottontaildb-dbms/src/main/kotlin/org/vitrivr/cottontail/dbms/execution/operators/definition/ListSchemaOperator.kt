package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.AccessMode
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.ColumnSets
import org.vitrivr.cottontail.dbms.schema.Schema

/**
 * An [Operator.SourceOperator] used during query execution. Lists all available [Schema]s.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class ListSchemaOperator(override val context: QueryContext) : Operator.SourceOperator() {
    override val columns: List<ColumnDef<*>> = ColumnSets.DDL_LIST_COLUMNS
    override fun toFlow(): Flow<Tuple> = flow {
        val catalogueTx = this@ListSchemaOperator.context.transaction.catalogueTx(AccessMode.READ)
        val columns = this@ListSchemaOperator.columns.toTypedArray()
        for ((i, schema) in catalogueTx.listSchemas().withIndex()) {
            emit(StandaloneTuple(i + 1L, columns, arrayOf(StringValue(schema.fqn), StringValue("SCHEMA"))))
        }
    }
}