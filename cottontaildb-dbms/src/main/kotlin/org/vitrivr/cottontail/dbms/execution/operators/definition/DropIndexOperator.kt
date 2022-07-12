package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.schema.SchemaTx
import kotlin.system.measureTimeMillis

/**
 * An [Operator.SourceOperator] used during query execution. Drops an [Index]
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class DropIndexOperator(private val tx: CatalogueTx, private val name: Name.IndexName): AbstractDataDefinitionOperator(name, "DROP INDEX") {
    override fun toFlow(context: TransactionContext): Flow<Record> = flow {
        val schemaTxn = context.getTx(this@DropIndexOperator.tx.schemaForName(this@DropIndexOperator.name.schema())) as SchemaTx
        val entityTxn = context.getTx(schemaTxn.entityForName(this@DropIndexOperator.name.entity())) as EntityTx
        val time = measureTimeMillis {
            entityTxn.dropIndex(this@DropIndexOperator.name)
        }
        emit(this@DropIndexOperator.statusRecord(time))
    }
}