package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.AccessMode
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.ColumnSets

/**
 * An [Operator.SourceOperator] used during query execution. Retrieves information about an index.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class AboutIndexOperator(private val name: Name.IndexName, override val context: QueryContext) : Operator.SourceOperator() {
    override val columns: List<ColumnDef<*>> = ColumnSets.DDL_INTROSPECTION_COLUMNS
    override fun toFlow(): Flow<Tuple> = flow {
        val indexTxn = this@AboutIndexOperator.context.transaction.indexTx(this@AboutIndexOperator.name, AccessMode.READ)
        val config = indexTxn.config.toMap()
        val columns = this@AboutIndexOperator.columns.toTypedArray()
        var rowId = 1L
        for ((k,v) in config) {
            emit(StandaloneTuple(rowId++, columns, arrayOf(StringValue(this@AboutIndexOperator.name.fqn), StringValue(k), StringValue(v))))
        }
    }
}