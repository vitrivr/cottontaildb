package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.schema.Schema
import kotlin.system.measureTimeMillis

/**
 * An [Operator.SourceOperator] used during query execution. Drops a [Schema]
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class DropSchemaOperator(private val tx: CatalogueTx, private val name: Name.SchemaName, override val context: QueryContext) : AbstractDataDefinitionOperator(name, "DROP SCHEMA") {
    override fun toFlow(): Flow<Tuple> = flow {
        val time = measureTimeMillis {
            this@DropSchemaOperator.tx.dropSchema(this@DropSchemaOperator.name)
        }
        emit(this@DropSchemaOperator.statusRecord(time))
    }
}