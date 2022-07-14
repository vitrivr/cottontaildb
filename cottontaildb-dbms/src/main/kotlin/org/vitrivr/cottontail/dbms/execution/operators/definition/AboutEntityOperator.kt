package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.BooleanValue
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.column.ColumnTx
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.index.basic.IndexTx
import org.vitrivr.cottontail.dbms.queries.operators.ColumnSets
import org.vitrivr.cottontail.dbms.schema.SchemaTx

/**
 * An [Operator.SourceOperator] used during query execution. Retrieves information about an entity.
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
class AboutEntityOperator(private val tx: CatalogueTx, private val name: Name.EntityName) : Operator.SourceOperator() {
    override val columns: List<ColumnDef<*>> = ColumnSets.DDL_ABOUT_COLUMNS

    override fun toFlow(context: TransactionContext): Flow<Record> = flow {
        val schemaTxn = context.getTx(this@AboutEntityOperator.tx.schemaForName(this@AboutEntityOperator.name.schema())) as SchemaTx
        val entityTxn = context.getTx(schemaTxn.entityForName(this@AboutEntityOperator.name)) as EntityTx

        val columns = this@AboutEntityOperator.columns.toTypedArray()
        var rowId = 0L

        /* Describe entity. */
        emit(StandaloneRecord(rowId++, columns, arrayOf(
            StringValue(this@AboutEntityOperator.name.toString()),
            StringValue("ENTITY"),
            null,
            IntValue(entityTxn.count()),
            null,
            null,
            null
        )))

        /* Describe columns. */
        val cols = entityTxn.listColumns()
        cols.forEach {
            val column = entityTxn.columnForName(it.name)
            val columnTx = context.getTx(column) as ColumnTx<*>
            emit(StandaloneRecord(rowId++, columns, arrayOf(
                StringValue(it.name.toString()),
                StringValue("COLUMN"),
                StringValue(it.type.name),
                IntValue(columnTx.count()),
                IntValue(it.type.logicalSize),
                BooleanValue(it.nullable),
                if (it.primary) {
                    StringValue("PRIMARY KEY")
                } else {
                    null
                }
            )))
        }

        /* Describe indexes. */
        val indexes = entityTxn.listIndexes()
        indexes.forEach {
            val index = entityTxn.indexForName(it)
            val indexTx = context.getTx(index) as IndexTx
            emit(StandaloneRecord(rowId++, columns, arrayOf(
                StringValue(it.toString()),
                StringValue("INDEX"),
                StringValue(index.type.toString()),
                IntValue(indexTx.count()),
                null,
                null,
                StringValue(indexTx.state.toString())
            )))
        }
    }
}