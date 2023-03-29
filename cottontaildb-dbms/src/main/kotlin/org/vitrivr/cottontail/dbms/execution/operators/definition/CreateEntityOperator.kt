package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import kotlin.system.measureTimeMillis

/**
 * An [Operator.SourceOperator] used during query execution. Creates an [Entity]
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class CreateEntityOperator(private val tx: CatalogueTx, private val name: Name.EntityName, private val cols: Array<ColumnDef<*>>, override val context: QueryContext) : AbstractDataDefinitionOperator(name, "CREATE ENTITY") {

    override fun toFlow(): Flow<Record> = flow {
        val schemaTxn = this@CreateEntityOperator.tx.schemaForName(this@CreateEntityOperator.name.schema()).newTx(this@CreateEntityOperator.context)
        val time = measureTimeMillis {
            schemaTxn.createEntity(this@CreateEntityOperator.name, *this@CreateEntityOperator.cols)
        }
        emit(this@CreateEntityOperator.statusRecord(time))
    }
}