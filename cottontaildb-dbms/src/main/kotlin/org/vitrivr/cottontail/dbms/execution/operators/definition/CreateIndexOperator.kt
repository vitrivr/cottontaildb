package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.index.basic.IndexType
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import kotlin.system.measureTimeMillis

/**
 * An [Operator.SourceOperator] used during query execution. Creates an [Index]
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
class CreateIndexOperator(
    private val tx: CatalogueTx,
    private val name: Name.IndexName,
    private val type: IndexType,
    private val indexColumns: List<Name.ColumnName>,
    private val params: Map<String, String>,
    override val context: QueryContext
) : AbstractDataDefinitionOperator(name, "CREATE INDEX") {
    override fun toFlow(): Flow<Tuple> = flow {
        val time = measureTimeMillis {
            val schemaTxn = this@CreateIndexOperator.tx.schemaForName(this@CreateIndexOperator.name.schema()).newTx(this@CreateIndexOperator.tx)
            val entityTxn = schemaTxn.entityForName(this@CreateIndexOperator.name.entity()).createOrResumeTx(schemaTxn)
            val columns = this@CreateIndexOperator.indexColumns.map { entityTxn.columnForName(it).columnDef.name }
            entityTxn.createIndex(this@CreateIndexOperator.name, this@CreateIndexOperator.type, columns, this@CreateIndexOperator.type.descriptor.buildConfig(this@CreateIndexOperator.params))
        }
        emit(this@CreateIndexOperator.statusRecord(time))
    }
}