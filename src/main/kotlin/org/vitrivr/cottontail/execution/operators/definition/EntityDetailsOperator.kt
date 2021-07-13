package org.vitrivr.cottontail.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.catalogue.CatalogueTx
import org.vitrivr.cottontail.database.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.schema.SchemaTx
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.BooleanValue
import org.vitrivr.cottontail.model.values.IntValue
import org.vitrivr.cottontail.model.values.StringValue
import org.vitrivr.cottontail.model.values.types.Value
import kotlin.time.ExperimentalTime

/**
 * An [Operator.SourceOperator] used during query execution. Retrieves information regarding entities.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class EntityDetailsOperator(val catalogue: DefaultCatalogue, val name: Name.EntityName) : Operator.SourceOperator() {

    companion object {
        val COLUMNS: List<ColumnDef<*>> = listOf(
            ColumnDef(Name.ColumnName("dbo"), Type.String, false),
            ColumnDef(Name.ColumnName("class"), Type.String, false),
            ColumnDef(Name.ColumnName("type"), Type.String, true),
            ColumnDef(Name.ColumnName("rows"), Type.Int, true),
            ColumnDef(Name.ColumnName("l_size"), Type.Int, true),
            ColumnDef(Name.ColumnName("nullable"), Type.Boolean, true)
        )
    }

    override val columns: List<ColumnDef<*>> = COLUMNS

    @ExperimentalTime
    override fun toFlow(context: QueryContext): Flow<Record> {
        val catTxn = context.txn.getTx(this.catalogue) as CatalogueTx
        val schemaTxn = context.txn.getTx(catTxn.schemaForName(this.name.schema())) as SchemaTx
        val entityTxn = context.txn.getTx(schemaTxn.entityForName(this.name)) as EntityTx
        val columns = this.columns.toTypedArray()
        val values = arrayOfNulls<Value?>(this.columns.size)
        return flow {
            var rowId = 0L
            values[0] = StringValue(this@EntityDetailsOperator.name.toString())
            values[1] = StringValue("ENTITY")
            values[3] =  IntValue(entityTxn.count())
            emit(StandaloneRecord(rowId++, columns, values))

            val cols = entityTxn.listColumns()
            values[1] =  StringValue("COLUMN")
            values[3] = null
            cols.forEach {
                values[0] = StringValue(it.name.toString())
                values[2] = StringValue(it.type.toString())
                values[4] = IntValue(it.columnDef.type.logicalSize)
                values[5] =  BooleanValue(it.nullable)
                emit(StandaloneRecord(rowId++, columns, values))
            }

            val indexes = entityTxn.listIndexes()
            values[1] =  StringValue("INDEX")
            values[3] = null
            values[4] = null
            values[5] = null
            indexes.forEach {
                values[0] = StringValue(it.name.toString())
                values[2] = StringValue(it.type.toString())
                emit(StandaloneRecord(rowId++, columns, values))
            }
        }
    }
}