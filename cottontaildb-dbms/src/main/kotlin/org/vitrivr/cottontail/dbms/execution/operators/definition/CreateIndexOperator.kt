package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.index.basic.IndexType
import org.vitrivr.cottontail.dbms.schema.SchemaTx
import kotlin.system.measureTimeMillis

/**
 * An [Operator.SourceOperator] used during query execution. Creates an [Index]
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class CreateIndexOperator(
    private val tx: CatalogueTx,
    private val name: Name.IndexName,
    private val type: IndexType,
    private val indexColumns: List<Name.ColumnName>,
    private val params: Map<String, String>
) : AbstractDataDefinitionOperator(name, "CREATE INDEX") {
    override fun toFlow(context: TransactionContext): Flow<Record> = flow {
        val time = measureTimeMillis {
            val schemaTxn = context.getTx(this@CreateIndexOperator.tx.schemaForName(this@CreateIndexOperator.name.schema())) as SchemaTx
            val entityTxn = context.getTx(schemaTxn.entityForName(this@CreateIndexOperator.name.entity())) as EntityTx
            val columns = this@CreateIndexOperator.indexColumns.map { entityTxn.columnForName(it).columnDef.name }
            entityTxn.createIndex(this@CreateIndexOperator.name, this@CreateIndexOperator.type, columns, this@CreateIndexOperator.type.descriptor.buildConfig(this@CreateIndexOperator.params))
        }
        emit(this@CreateIndexOperator.statusRecord(time))
    }
}