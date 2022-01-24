package org.vitrivr.cottontail.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.schema.SchemaTx
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.basics.Record
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * An [Operator.SourceOperator] used during query execution. Truncates an [Entity] (i.e. drops and re-creates it).
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
@ExperimentalTime
class TruncateEntityOperator(val catalogue: Catalogue, val name: Name.EntityName) : AbstractDataDefinitionOperator(name, "TRUNCATE ENTITY") {
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val catTxn = context.getTx(this.catalogue) as CatalogueTx
        val schemaTxn = context.getTx(catTxn.schemaForName(this.name.schema())) as SchemaTx
        val entityTxn = context.getTx(schemaTxn.entityForName(this.name)) as EntityTx
        return flow {
            val columns =
                entityTxn.listColumns().map { Pair(it.columnDef, it.engine) }.toTypedArray()
            val timedTupleId = measureTimedValue {
                schemaTxn.dropEntity(this@TruncateEntityOperator.name)
                schemaTxn.createEntity(this@TruncateEntityOperator.name, *columns)
            }
            emit(this@TruncateEntityOperator.statusRecord(timedTupleId.duration))
        }
    }
}