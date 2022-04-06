package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.schema.Schema
import kotlin.system.measureTimeMillis

/**
 * An [Operator.SourceOperator] used during query execution. Creates a new [Schema]
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class CreateSchemaOperator(private val tx: CatalogueTx, private val name: Name.SchemaName) : AbstractDataDefinitionOperator(name, "CREATE SCHEMA") {

    override fun toFlow(context: TransactionContext): Flow<Record> = flow {
        val duration = measureTimeMillis {
            this@CreateSchemaOperator.tx.createSchema(this@CreateSchemaOperator.name)
        }
        emit(this@CreateSchemaOperator.statusRecord(duration))
    }
}