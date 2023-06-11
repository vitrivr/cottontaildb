package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.schema.Schema
import kotlin.system.measureTimeMillis

/**
 * An [Operator.SourceOperator] used during query execution. Creates a new [Schema]
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
class CreateSchemaOperator(private val tx: CatalogueTx, private val name: Name.SchemaName, private val mayExist: Boolean, override val context: QueryContext) : AbstractDataDefinitionOperator(name, "CREATE SCHEMA") {

    override fun toFlow(): Flow<Tuple> = flow {
        val duration = measureTimeMillis {
            try {
                this@CreateSchemaOperator.tx.createSchema(this@CreateSchemaOperator.name)
            } catch (e: DatabaseException.SchemaAlreadyExistsException) {
                if (!this@CreateSchemaOperator.mayExist) throw e
            }
        }
        emit(this@CreateSchemaOperator.statusRecord(duration))
    }
}