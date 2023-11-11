package org.vitrivr.cottontail.dbms.execution.operators.definition

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.values.StringValue
import org.vitrivr.cottontail.core.values.Value
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.AccessMode
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.ColumnSets
import org.vitrivr.cottontail.dbms.statistics.defaultStatistics

/**
 * An [Operator.SourceOperator] used during query execution. Retrieves and returns statistics about entities.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class EntityStatisticsOperator(private val entity: Name.EntityName, override val context: QueryContext) : Operator.SourceOperator() {
    override val columns: List<ColumnDef<*>> = ColumnSets.DDL_INTROSPECTION_COLUMNS
    override fun toFlow(): Flow<Tuple> = flow {
        val entityTxn = this@EntityStatisticsOperator.context.transaction.entityTx(this@EntityStatisticsOperator.entity, AccessMode.READ)

        /* Describe columns. */
        var rowId = 0L
        entityTxn.listColumns().forEach {
            val statistics = this@EntityStatisticsOperator.context.transaction.manager.statistics[it.name]?.statistics ?: it.type.defaultStatistics<Value>()
            for ((k,v) in statistics.about()) {
                emit(StandaloneTuple(rowId++, columns.toTypedArray(), arrayOf(StringValue(it.name.fqn), StringValue(k), StringValue(v))))
            }
        }
    }
}