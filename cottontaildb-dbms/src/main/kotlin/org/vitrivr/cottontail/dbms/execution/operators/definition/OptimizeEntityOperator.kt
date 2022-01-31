package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.schema.SchemaTx
import org.vitrivr.cottontail.dbms.execution.TransactionContext
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.basics.Record
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * An [Operator.SourceOperator] used during query execution. Optimizes an [Entity]
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
@ExperimentalTime
class OptimizeEntityOperator(private val catalogue: DefaultCatalogue, private val name: Name.EntityName) : AbstractDataDefinitionOperator(name, "OPTIMIZE ENTITY") {
    override fun toFlow(context: org.vitrivr.cottontail.dbms.execution.TransactionContext): Flow<Record> {
        val catTxn = context.getTx(this.catalogue) as CatalogueTx
        val schemaTxn = context.getTx(catTxn.schemaForName(this.name.schema())) as SchemaTx
        val entityTxn = context.getTx(schemaTxn.entityForName(this.name)) as EntityTx
        return flow {
            val timedTupleId = measureTimedValue { entityTxn.optimize() }
            emit(this@OptimizeEntityOperator.statusRecord(timedTupleId.duration))
        }
    }
}