package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.AccessMode
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.ColumnSets

/**
 * An [Operator.SourceOperator] used during query execution. Lists all available [Entity]s.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class ListEntityOperator(private val schema: Name.SchemaName? = null, override val context: QueryContext) : Operator.SourceOperator() {
    override val columns: List<ColumnDef<*>> = ColumnSets.DDL_LIST_COLUMNS
    override fun toFlow(): Flow<Tuple> = flow {
        val schemas = if (this@ListEntityOperator.schema != null) {
            listOf(this@ListEntityOperator.schema)
        } else {
            val catalogueTx = this@ListEntityOperator.context.transaction.catalogueTx(AccessMode.READ)
            catalogueTx.listSchemas()
        }
        val columns = this@ListEntityOperator.columns.toTypedArray()
        var i = 0L
        for (schema in schemas) {
            val schemaTxn = this@ListEntityOperator.context.transaction.schemaTx(schema, AccessMode.READ)
            for (entity in schemaTxn.listEntities()) {
                emit(StandaloneTuple(i++, columns, arrayOf(StringValue(entity.fqn),StringValue("ENTITY"))))
            }
        }
    }
}