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
 * An [Operator.SourceOperator] used during query execution. Can be used to optimize a single [Index]
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class RebuildIndexOperator(private val tx: CatalogueTx, private val name: Name.IndexName, override val context: QueryContext): AbstractDataDefinitionOperator(name, "OPTIMIZE INDEX") {
    override fun toFlow(): Flow<Record> = flow {
        val schemaTxn = this@RebuildIndexOperator.tx.schemaForName(this@RebuildIndexOperator.name.schema()).newTx(this@RebuildIndexOperator.context)
        val entityTxn = schemaTxn.entityForName(this@RebuildIndexOperator.name.entity()).newTx(this@RebuildIndexOperator.context)
        val rebuilder = entityTxn.indexForName(this@RebuildIndexOperator.name).newRebuilder(this@RebuildIndexOperator.context)
        val time = measureTimeMillis { rebuilder.rebuild() }
        emit(this@RebuildIndexOperator.statusRecord(time))
    }
}