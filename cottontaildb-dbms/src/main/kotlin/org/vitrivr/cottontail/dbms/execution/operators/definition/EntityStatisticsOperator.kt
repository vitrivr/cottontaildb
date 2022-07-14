package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.column.ColumnTx
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.queries.operators.ColumnSets
import org.vitrivr.cottontail.dbms.schema.SchemaTx

/**
 * An [Operator.SourceOperator] used during query execution. Retrieves and returns statistics about entities.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class EntityStatisticsOperator(private val tx: CatalogueTx, private val name: Name.EntityName) : Operator.SourceOperator() {
    override val columns: List<ColumnDef<*>> = ColumnSets.DDL_INTROSPECTION_COLUMNS
    override fun toFlow(context: TransactionContext): Flow<Record> = flow {
        val schemaTxn = context.getTx(this@EntityStatisticsOperator.tx.schemaForName(this@EntityStatisticsOperator.name.schema())) as SchemaTx
        val entityTxn = context.getTx(schemaTxn.entityForName(this@EntityStatisticsOperator.name)) as EntityTx

        /* Describe columns. */
        var rowId = 0L
        val cols = entityTxn.listColumns()
        cols.forEach {
            val column = entityTxn.columnForName(it.name)
            val columnTx = context.getTx(column) as ColumnTx<*>
            val statistics = columnTx.statistics().about()
            for ((k,v) in statistics) {
                emit(StandaloneRecord(rowId++, columns.toTypedArray(), arrayOf(StringValue(column.name.fqn), StringValue(k), StringValue(v))))
            }
        }
    }
}