package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import kotlin.system.measureTimeMillis

/**
 * An [Operator.SourceOperator] used during query execution. Drops an [Index]
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class DropIndexOperator(private val tx: CatalogueTx, private val name: Name.IndexName, override val context: QueryContext): AbstractDataDefinitionOperator(name, "DROP INDEX") {
    override fun toFlow(): Flow<Record> = flow {
        val schemaTxn = this@DropIndexOperator.tx.schemaForName(this@DropIndexOperator.name.schema()).newTx(this@DropIndexOperator.context)
        val entityTxn = schemaTxn.entityForName(this@DropIndexOperator.name.entity()).newTx(this@DropIndexOperator.context)
        val time = measureTimeMillis {
            entityTxn.dropIndex(this@DropIndexOperator.name)
        }
        emit(this@DropIndexOperator.statusRecord(time))
    }
}