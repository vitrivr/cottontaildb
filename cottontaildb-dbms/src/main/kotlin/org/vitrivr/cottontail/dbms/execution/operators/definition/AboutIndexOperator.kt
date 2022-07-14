package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.index.basic.IndexTx
import org.vitrivr.cottontail.dbms.queries.operators.ColumnSets
import org.vitrivr.cottontail.dbms.schema.SchemaTx

/**
 * An [Operator.SourceOperator] used during query execution. Retrieves information about an index.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class AboutIndexOperator(private val tx: CatalogueTx, private val name: Name.IndexName) : Operator.SourceOperator() {
    override val columns: List<ColumnDef<*>> = ColumnSets.DDL_INTROSPECTION_COLUMNS
    override fun toFlow(context: TransactionContext): Flow<Record> = flow {
        val schemaTxn = context.getTx(this@AboutIndexOperator.tx.schemaForName(this@AboutIndexOperator.name.schema())) as SchemaTx
        val entityTxn = context.getTx(schemaTxn.entityForName(this@AboutIndexOperator.name.entity())) as EntityTx
        val indexTxn = context.getTx(entityTxn.indexForName(this@AboutIndexOperator.name)) as IndexTx
        val config = indexTxn.config.toMap()
        val columns = this@AboutIndexOperator.columns.toTypedArray()
        var rowId = 1L
        for ((k,v) in config) {
            emit(StandaloneRecord(rowId++, columns, arrayOf(StringValue(this@AboutIndexOperator.name.fqn), StringValue(k), StringValue(v))))
        }
    }
}