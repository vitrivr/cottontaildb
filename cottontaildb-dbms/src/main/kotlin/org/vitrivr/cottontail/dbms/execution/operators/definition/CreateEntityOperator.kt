package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.column.ColumnMetadata
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.AccessMode
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import kotlin.system.measureTimeMillis

/**
 * An [Operator.SourceOperator] used during query execution. Creates an [Entity]
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class CreateEntityOperator(private val name: Name.EntityName, private val cols: Map<Name.ColumnName, ColumnMetadata>, private val mayExist: Boolean, override val context: QueryContext) : AbstractDataDefinitionOperator(name, "CREATE ENTITY") {

    override fun toFlow(): Flow<Tuple> = flow {
        val schemaTxn = this@CreateEntityOperator.context.transaction.schemaTx(this@CreateEntityOperator.name.schema(), AccessMode.WRITE)
        val time = measureTimeMillis {
            try {
                schemaTxn.createEntity(this@CreateEntityOperator.name, this@CreateEntityOperator.cols)
            } catch (e: Throwable) {
                if (!this@CreateEntityOperator.mayExist) throw e
            }
        }
        emit(this@CreateEntityOperator.statusRecord(time))
    }
}