package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.index.IndexTx
import org.vitrivr.cottontail.dbms.index.IndexType
import org.vitrivr.cottontail.dbms.schema.SchemaTx
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * An [Operator.SourceOperator] used during query execution. Creates an [Index]
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
@ExperimentalTime
class CreateIndexOperator(
    private val catalogue: DefaultCatalogue,
    private val name: Name.IndexName,
    private val type: IndexType,
    private val indexColumns: List<Name.ColumnName>,
    private val params: Map<String, String>,
    private val rebuild: Boolean = false,
) : AbstractDataDefinitionOperator(name, "CREATE INDEX") {

    override fun toFlow(context: org.vitrivr.cottontail.dbms.execution.TransactionContext): Flow<Record> {
        val catTxn = context.getTx(this.catalogue) as CatalogueTx
        val schemaTxn = context.getTx(catTxn.schemaForName(this.name.schema())) as SchemaTx
        val entityTxn = context.getTx(schemaTxn.entityForName(this.name.entity())) as EntityTx
        val columns = this.indexColumns.map { entityTxn.columnForName(it).columnDef }.toTypedArray()
        return flow {
            val timedTupleId = measureTimedValue {
                val index = entityTxn.createIndex(
                    this@CreateIndexOperator.name,
                    this@CreateIndexOperator.type,
                    columns,
                    this@CreateIndexOperator.params
                )
                if (this@CreateIndexOperator.rebuild) {
                    val indexTxn = context.getTx(index) as IndexTx
                    indexTxn.rebuild()
                }
            }
            emit(this@CreateIndexOperator.statusRecord(timedTupleId.duration))
        }
    }
}