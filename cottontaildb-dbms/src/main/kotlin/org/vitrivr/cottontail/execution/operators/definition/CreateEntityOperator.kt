package org.vitrivr.cottontail.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.column.ColumnEngine
import org.vitrivr.cottontail.dbms.schema.SchemaTx
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * An [Operator.SourceOperator] used during query execution. Creates an [Entity]
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
@ExperimentalTime
class CreateEntityOperator(
    private val catalogue: Catalogue,
    private val name: Name.EntityName,
    private val cols: Array<Pair<ColumnDef<*>, ColumnEngine>>
) : AbstractDataDefinitionOperator(name, "CREATE ENTITY") {

    override fun toFlow(context: TransactionContext): Flow<Record> {
        val catTxn = context.getTx(this.catalogue) as CatalogueTx
        val schemaTxn = context.getTx(catTxn.schemaForName(this.name.schema())) as SchemaTx
        return flow {
            val timedTupleId = measureTimedValue {
                schemaTxn.createEntity(
                    this@CreateEntityOperator.name,
                    *this@CreateEntityOperator.cols
                )
            }
            emit(this@CreateEntityOperator.statusRecord(timedTupleId.duration))
        }
    }
}