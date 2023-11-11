package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.AccessMode
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.schema.Schema
import kotlin.system.measureTimeMillis

/**
 * An [Operator.SourceOperator] used during query execution. Creates a new [Schema]
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class CreateSchemaOperator(private val name: Name.SchemaName, private val mayExist: Boolean, override val context: QueryContext) : AbstractDataDefinitionOperator(name, "CREATE SCHEMA") {

    override fun toFlow(): Flow<Tuple> = flow {
        val duration = measureTimeMillis {
            try {
                val catalogueTx = this@CreateSchemaOperator.context.transaction.catalogueTx(AccessMode.WRITE)
                catalogueTx.createSchema(this@CreateSchemaOperator.name)
            } catch (e: DatabaseException.SchemaAlreadyExistsException) {
                if (!this@CreateSchemaOperator.mayExist) throw e
            }
        }
        emit(this@CreateSchemaOperator.statusRecord(duration))
    }
}