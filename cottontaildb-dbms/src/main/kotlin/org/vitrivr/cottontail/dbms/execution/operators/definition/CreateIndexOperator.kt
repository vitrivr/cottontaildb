package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.index.Index
import org.vitrivr.cottontail.dbms.index.IndexTx
import org.vitrivr.cottontail.dbms.index.IndexType
import org.vitrivr.cottontail.dbms.schema.SchemaTx
import kotlin.system.measureTimeMillis

/**
 * An [Operator.SourceOperator] used during query execution. Creates an [Index]
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class CreateIndexOperator(
    private val tx: CatalogueTx,
    private val name: Name.IndexName,
    private val type: IndexType,
    private val indexColumns: List<Name.ColumnName>,
    private val params: Map<String, String>,
    private val rebuild: Boolean = false,
) : AbstractDataDefinitionOperator(name, "CREATE INDEX") {
    override fun toFlow(context: TransactionContext): Flow<Record> = flow {
        val time = measureTimeMillis {
            val schemaTxn = context.getTx(this@CreateIndexOperator.tx.schemaForName(this@CreateIndexOperator.name.schema())) as SchemaTx
            val entityTxn = context.getTx(schemaTxn.entityForName(this@CreateIndexOperator.name.entity())) as EntityTx
            val columns = this@CreateIndexOperator.indexColumns.map { entityTxn.columnForName(it).columnDef.name }
            val index = entityTxn.createIndex(
                this@CreateIndexOperator.name,
                this@CreateIndexOperator.type,
                columns,
                this@CreateIndexOperator.type.descriptor.buildConfig(this@CreateIndexOperator.params)
            )
            if (this@CreateIndexOperator.rebuild) {
                val indexTxn = context.getTx(index) as IndexTx
                indexTxn.rebuild()
            }
        }
        emit(this@CreateIndexOperator.statusRecord(time))
    }
}