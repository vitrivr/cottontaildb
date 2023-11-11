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
 * An [Operator.SourceOperator] used during query execution. Truncates an [Entity] (i.e., drops and re-creates it).
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class TruncateEntityOperator(private val name: Name.EntityName, override val context: QueryContext) : AbstractDataDefinitionOperator(name, "TRUNCATE ENTITY") {
    override fun toFlow(): Flow<Tuple> = flow {
        val entityTxn = this@TruncateEntityOperator.context.transaction.entityTx(this@TruncateEntityOperator.name, AccessMode.WRITE)
        val time = measureTimeMillis {
            entityTxn.truncate()
        }
        emit(this@TruncateEntityOperator.statusRecord(time))
    }
}