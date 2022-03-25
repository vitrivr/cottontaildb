package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * An [Operator.SourceOperator] used during query execution. Creates a new [Schema]
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
@ExperimentalTime
class CreateSchemaOperator(val catalogue: DefaultCatalogue, val name: Name.SchemaName) : AbstractDataDefinitionOperator(name, "CREATE SCHEMA") {

    override fun toFlow(context: TransactionContext): Flow<Record> {
        val txn = context.getTx(this.catalogue) as CatalogueTx
        return flow {
            val timedTupleId = measureTimedValue {
                txn.createSchema(this@CreateSchemaOperator.name)
            }
            emit(this@CreateSchemaOperator.statusRecord(timedTupleId.duration))
        }
    }
}