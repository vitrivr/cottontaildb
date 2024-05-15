package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.ColumnSets

/**
 * An [Operator.SourceOperator] used during query execution. Retrieves and returns statistics about entities.
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
class EntityStatisticsOperator(private val tx: CatalogueTx, private val name: Name.EntityName, override val context: QueryContext) : Operator.SourceOperator() {
    override val columns: List<ColumnDef<*>> = ColumnSets.DDL_INTROSPECTION_COLUMNS
    override fun toFlow(): Flow<Tuple> = flow {
        val schemaTxn = this@EntityStatisticsOperator.tx.schemaForName(this@EntityStatisticsOperator.name.schema()).newTx(this@EntityStatisticsOperator.tx)
        val entityTxn = schemaTxn.entityForName(this@EntityStatisticsOperator.name).createOrResumeTx(schemaTxn)

        /* Describe columns. */
        var rowId = 0L
        val cols = entityTxn.listColumns()
        cols.forEach {
            val statistics = entityTxn.statistics(it).about()
            for ((k,v) in statistics) {
                emit(StandaloneTuple(rowId++, columns.toTypedArray(), arrayOf(StringValue(it.name.fqn), StringValue(k), StringValue(v))))
            }
        }
    }
}