package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.AccessMode
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.index.basic.IndexType
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import kotlin.system.measureTimeMillis

/**
 * An [Operator.SourceOperator] used during query execution. Creates an [Index]
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class CreateIndexOperator(
    private val name: Name.IndexName,
    private val type: IndexType,
    private val indexColumns: List<Name.ColumnName>,
    private val params: Map<String, String>,
    override val context: QueryContext
) : AbstractDataDefinitionOperator(name, "CREATE INDEX") {
    override fun toFlow(): Flow<Tuple> = flow {
        val time = measureTimeMillis {
            val entityTxn = this@CreateIndexOperator.context.transaction.entityTx(this@CreateIndexOperator.name.entity(), AccessMode.WRITE)
            val columns = this@CreateIndexOperator.indexColumns.map { entityTxn.columnForName(it).columnDef.name }
            entityTxn.createIndex(this@CreateIndexOperator.name, this@CreateIndexOperator.type, columns, this@CreateIndexOperator.type.descriptor.buildConfig(this@CreateIndexOperator.params))
        }
        emit(this@CreateIndexOperator.statusRecord(time))
    }
}