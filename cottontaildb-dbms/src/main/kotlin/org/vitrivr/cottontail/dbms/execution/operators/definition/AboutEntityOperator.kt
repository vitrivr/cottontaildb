package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.values.BooleanValue
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.AccessMode
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.ColumnSets

/**
 * An [Operator.SourceOperator] used during query execution. Retrieves information about an entity.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class AboutEntityOperator(private val name: Name.EntityName, override val context: QueryContext) : Operator.SourceOperator() {
    override val columns: List<ColumnDef<*>> = ColumnSets.DDL_ABOUT_COLUMNS

    override fun toFlow(): Flow<Tuple> = flow {
        val entityTxn = this@AboutEntityOperator.context.transaction.entityTx(this@AboutEntityOperator.name, AccessMode.READ)
        val columns = this@AboutEntityOperator.columns.toTypedArray()
        var rowId = 0L

        /* Describe entity. */
        emit(
            StandaloneTuple(rowId++, columns, arrayOf(
                StringValue(this@AboutEntityOperator.name.toString()),
                StringValue("ENTITY"),
                null,
                LongValue(entityTxn.count()),
                null,
                null,
                null
            ))
        )

        /* Describe columns. */
        val cols = entityTxn.listColumns()
        cols.forEach {
            emit(
                StandaloneTuple(rowId++, columns, arrayOf(
                StringValue(it.name.toString()),
                StringValue("COLUMN"),
                StringValue(it.type.name),
                LongValue(entityTxn.count()),
                IntValue(it.type.logicalSize),
                BooleanValue(it.nullable),
                if (it.primary) {
                    StringValue("PRIMARY KEY")
                } else {
                    null
                }
            ))
            )
        }

        /* Describe indexes. */
        val indexes = entityTxn.listIndexes()
        indexes.forEach {
            val indexTx = this@AboutEntityOperator.context.transaction.indexTx(it, AccessMode.READ)
            emit(
                StandaloneTuple(rowId++, columns, arrayOf(
                StringValue(it.toString()),
                StringValue("INDEX"),
                StringValue(indexTx.dbo.type.toString()),
                LongValue(indexTx.count()),
                null,
                null,
                StringValue(indexTx.state.toString())
            ))
            )
        }
    }
}