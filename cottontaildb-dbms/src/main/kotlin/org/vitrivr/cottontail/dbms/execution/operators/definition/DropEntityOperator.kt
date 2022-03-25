package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.schema.SchemaTx
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * An [Operator.SourceOperator] used during query execution. Drops an [Entity]
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
@ExperimentalTime
class DropEntityOperator(val catalogue: DefaultCatalogue, val name: Name.EntityName) : AbstractDataDefinitionOperator(name, "DROP ENTITY") {
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val catTxn = context.getTx(this.catalogue) as CatalogueTx
        val schemaTxn = context.getTx(catTxn.schemaForName(this.name.schema())) as SchemaTx
        return flow {
            val timedTupleId = measureTimedValue {
                schemaTxn.dropEntity(this@DropEntityOperator.name)
            }
            emit(this@DropEntityOperator.statusRecord(timedTupleId.duration))
        }
    }
}