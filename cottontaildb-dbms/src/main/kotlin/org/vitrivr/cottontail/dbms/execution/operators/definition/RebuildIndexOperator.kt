package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.services.AutoRebuilderService
import org.vitrivr.cottontail.dbms.execution.transactions.AccessMode
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import kotlin.system.measureTimeMillis

/**
 * An [Operator.SourceOperator] used during query execution. Can be used to optimize a single [Index]
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class RebuildIndexOperator(private val name: Name.IndexName, private val service: AutoRebuilderService? = null, override val context: QueryContext): AbstractDataDefinitionOperator(name, "OPTIMIZE INDEX") {
    override fun toFlow(): Flow<Tuple> = flow {
        val entityTxn = this@RebuildIndexOperator.context.transaction.entityTx(this@RebuildIndexOperator.name.entity(), AccessMode.READ)
        val index = entityTxn.indexForName(this@RebuildIndexOperator.name)
        val time = measureTimeMillis {
            if (this@RebuildIndexOperator.service != null) {
                this@RebuildIndexOperator.service.schedule(this@RebuildIndexOperator.name, index.type)
            } else {
                index.newRebuilder(this@RebuildIndexOperator.context).rebuild()
            }
        }
        emit(this@RebuildIndexOperator.statusRecord(time))
    }
}