package org.vitrivr.cottontail.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.client.language.basics.Constants
import org.vitrivr.cottontail.database.catalogue.CatalogueTx
import org.vitrivr.cottontail.database.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.database.queries.binding.EmptyBindingContext
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.values.types.Types
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.StringValue
import org.vitrivr.cottontail.model.values.types.Value

/**
 * An [Operator.SourceOperator] used during query execution. Lists all available [Schema]s.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class ListSchemaOperator(val catalogue: DefaultCatalogue) : Operator.SourceOperator() {
    companion object {
        val COLUMNS: List<ColumnDef<*>> = listOf(
            ColumnDef(Name.ColumnName(Constants.COLUMN_NAME_DBO), Types.String, false),
            ColumnDef(Name.ColumnName(Constants.COLUMN_NAME_CLASS), Types.String, false)
        )
    }

    /** The [BindingContext] used [AbstractDataDefinitionOperator]. */
    override val binding: BindingContext = EmptyBindingContext

    override val columns: List<ColumnDef<*>> = COLUMNS

    override fun toFlow(context: TransactionContext): Flow<Record> {
        val txn = context.getTx(this.catalogue) as CatalogueTx
        val columns = this@ListSchemaOperator.columns.toTypedArray()
        val values = arrayOfNulls<Value?>(columns.size)
        values[1] = StringValue("SCHEMA")
        return flow {
            var i = 0L
            for (schema in txn.listSchemas()) {
                values[0] = StringValue(schema.name.toString())
                emit(StandaloneRecord(i++, columns, values))
            }
        }
    }
}