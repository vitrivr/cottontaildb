package org.vitrivr.cottontail.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.BooleanValue
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.queries.binding.EmptyBindingContext
import org.vitrivr.cottontail.dbms.schema.SchemaTx
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
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
            ColumnDef(Name.ColumnName("dbo"), Types.String, false),
            ColumnDef(Name.ColumnName("class"), Types.String, false),
            ColumnDef(Name.ColumnName("type"), Types.String, true),
            ColumnDef(Name.ColumnName("rows"), Types.Int, true),
            ColumnDef(Name.ColumnName("l_size"), Types.Int, true),
            ColumnDef(Name.ColumnName("nullable"), Types.Boolean, true)
        )
    }

    override val columns: List<ColumnDef<*>> = COLUMNS

    @ExperimentalTime
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val catTxn = context.getTx(this.catalogue) as CatalogueTx
        val schemaTxn = context.getTx(catTxn.schemaForName(this.name.schema())) as SchemaTx
        val entityTxn = context.getTx(schemaTxn.entityForName(this.name)) as EntityTx
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