package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.ColumnSets

/**
 * An [Operator.SourceOperator] used during query execution. Retrieves and returns statistics about entities.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class EntityStatisticsOperator(private val tx: CatalogueTx, private val name: Name.EntityName, override val context: QueryContext) : Operator.SourceOperator() {
    override val columns: List<ColumnDef<*>> = ColumnSets.DDL_INTROSPECTION_COLUMNS
    override fun toFlow(): Flow<Record> = flow {
        val schemaTxn = this@EntityStatisticsOperator.tx.schemaForName(this@EntityStatisticsOperator.name.schema()).newTx(this@EntityStatisticsOperator.context)
        val entityTxn = schemaTxn.entityForName(this@EntityStatisticsOperator.name).newTx(this@EntityStatisticsOperator.context)

        /* Describe columns. */
        var rowId = 0L
        val cols = entityTxn.listColumns()
        cols.forEach {
            val column = entityTxn.columnForName(it.name)
            val columnTx = column.newTx(this@EntityStatisticsOperator.context)
            val statistics = columnTx.statistics().about()
            for ((k,v) in statistics) {
                emit(StandaloneRecord(rowId++, columns.toTypedArray(), arrayOf(StringValue(column.name.fqn), StringValue(k), StringValue(v))))
            }
        }
    }
}