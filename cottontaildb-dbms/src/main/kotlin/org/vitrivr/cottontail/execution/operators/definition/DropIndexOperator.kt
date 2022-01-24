package org.vitrivr.cottontail.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.schema.SchemaTx
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * An [Operator.SourceOperator] used during query execution. Drops an [Index]
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
@ExperimentalTime
class DropIndexOperator(val catalogue: DefaultCatalogue, val name: Name.IndexName) :
    AbstractDataDefinitionOperator(name, "DROP INDEX") {
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val catTxn = context.getTx(this.catalogue) as CatalogueTx
        val schemaTxn = context.getTx(catTxn.schemaForName(this.name.schema())) as SchemaTx
        val entityTxn = context.getTx(schemaTxn.entityForName(this.name.entity())) as EntityTx
        return flow {
            val timedTupleId = measureTimedValue {
                entityTxn.dropIndex(this@DropIndexOperator.name)
            }
            emit(this@DropIndexOperator.statusRecord(timedTupleId.duration))
        }
    }
}