package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.AccessMode
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import kotlin.system.measureTimeMillis

/**
 * An [Operator.SourceOperator] used during query execution. Drops an [Index]
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class DropIndexOperator(private val index: Name.IndexName, override val context: QueryContext): AbstractDataDefinitionOperator(index, "DROP INDEX") {
    override fun toFlow(): Flow<Tuple> = flow {
        val indexTxn = this@DropIndexOperator.context.transaction.indexTx(this@DropIndexOperator.index, AccessMode.WRITE)
        val time = measureTimeMillis {
            indexTxn.drop()
        }
        emit(this@DropIndexOperator.statusRecord(time))
    }
}