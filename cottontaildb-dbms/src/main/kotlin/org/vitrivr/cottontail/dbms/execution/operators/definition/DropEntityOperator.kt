package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import kotlin.system.measureTimeMillis

/**
 * An [Operator.SourceOperator] used during query execution. Drops an [Entity]
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class DropEntityOperator(private val tx: CatalogueTx, private val name: Name.EntityName, override val context: QueryContext) : AbstractDataDefinitionOperator(name, "DROP ENTITY") {
    override fun toFlow(): Flow<Tuple> = flow {
        val schemaTxn = this@DropEntityOperator.tx.schemaForName(this@DropEntityOperator.name.schema()).newTx(this@DropEntityOperator.tx)
        val time = measureTimeMillis {
            schemaTxn.dropEntity(this@DropEntityOperator.name)
        }
        emit(this@DropEntityOperator.statusRecord(time))
    }
}