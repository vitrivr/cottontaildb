package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.AccessMode
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import kotlin.system.measureTimeMillis

/**
 * An [Operator.SourceOperator] used during query execution. Drops an [Entity]
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class DropEntityOperator(private val entity: Name.EntityName, override val context: QueryContext) : AbstractDataDefinitionOperator(entity, "DROP ENTITY") {
    override fun toFlow(): Flow<Tuple> = flow {
        val time = measureTimeMillis {
            val entityTxn = this@DropEntityOperator.context.transaction.entityTx(this@DropEntityOperator.entity, AccessMode.WRITE)
            entityTxn.drop()
        }
        emit(this@DropEntityOperator.statusRecord(time))
    }
}