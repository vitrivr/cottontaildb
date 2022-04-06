package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.index.Index
import org.vitrivr.cottontail.dbms.index.IndexTx
import org.vitrivr.cottontail.dbms.schema.SchemaTx
import kotlin.system.measureTimeMillis

/**
 * An [Operator.SourceOperator] used during query execution. Can be used to optimize a single [Index]
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class RebuildIndexOperator(private val tx: CatalogueTx, private val name: Name.IndexName): AbstractDataDefinitionOperator(name, "OPTIMIZE INDEX") {
    override fun toFlow(context: TransactionContext): Flow<Record> = flow {
        val schemaTxn = context.getTx(this@RebuildIndexOperator.tx.schemaForName(this@RebuildIndexOperator.name.schema())) as SchemaTx
        val entityTxn = context.getTx(schemaTxn.entityForName(this@RebuildIndexOperator.name.entity())) as EntityTx
        val indexTxn = context.getTx(entityTxn.indexForName(this@RebuildIndexOperator.name)) as IndexTx
        val time = measureTimeMillis { indexTxn.rebuild() }
        emit(this@RebuildIndexOperator.statusRecord(time))
    }
}