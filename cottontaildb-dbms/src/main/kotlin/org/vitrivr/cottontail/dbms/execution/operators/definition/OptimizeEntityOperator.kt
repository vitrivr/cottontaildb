package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.schema.SchemaTx
import kotlin.system.measureTimeMillis

/**
 * An [Operator.SourceOperator] used during query execution. Optimizes an [Entity]
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class OptimizeEntityOperator(private val tx: CatalogueTx, private val name: Name.EntityName) : AbstractDataDefinitionOperator(name, "OPTIMIZE ENTITY") {
    override fun toFlow(context: TransactionContext): Flow<Record> = flow {
        val schemaTxn = context.getTx(this@OptimizeEntityOperator.tx.schemaForName(this@OptimizeEntityOperator.name.schema())) as SchemaTx
        val entityTxn = context.getTx(schemaTxn.entityForName(this@OptimizeEntityOperator.name)) as EntityTx
        val time = measureTimeMillis { entityTxn.optimize() }
        emit(this@OptimizeEntityOperator.statusRecord(time))
    }
}