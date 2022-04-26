package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.queries.operators.ColumnSets
import org.vitrivr.cottontail.dbms.schema.SchemaTx

/**
 * An [Operator.SourceOperator] used during query execution. Lists all available [Entity]s.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class ListEntityOperator(private val tx: CatalogueTx, private val schema: Name.SchemaName? = null) : Operator.SourceOperator() {
    override val columns: List<ColumnDef<*>> = ColumnSets.DDL_LIST_COLUMNS
    override fun toFlow(context: TransactionContext): Flow<Record> = flow {
        val schemas = if (this@ListEntityOperator.schema != null) {
            listOf(this@ListEntityOperator.tx.schemaForName(this@ListEntityOperator.schema))
        } else {
            this@ListEntityOperator.tx.listSchemas().map { this@ListEntityOperator.tx.schemaForName(it) }
        }
        val columns = this@ListEntityOperator.columns.toTypedArray()
        var i = 0L
        for (schema in schemas) {
            val schemaTxn = context.getTx(schema) as SchemaTx
            for (entity in schemaTxn.listEntities()) {
                emit(StandaloneRecord(i++, columns, arrayOf(StringValue(entity.fqn),StringValue("ENTITY"))))
            }
        }
    }
}