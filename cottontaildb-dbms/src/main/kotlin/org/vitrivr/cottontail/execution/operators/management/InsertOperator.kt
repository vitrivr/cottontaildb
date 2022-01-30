package org.vitrivr.cottontail.execution.operators.management

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.dbms.queries.binding.EmptyBindingContext
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.definition.AbstractDataDefinitionOperator
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.core.values.types.Value
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * An [Operator.PipelineOperator] used during query execution. Inserts all incoming entries into an
 * [Entity] that it receives with the provided [Value].
 *
 * @author Ralph Gasser
 * @version 1.4.0
 */
class InsertOperator(groupId: GroupId, val entity: EntityTx, val records: List<Record>) : Operator.SourceOperator(groupId) {
    companion object {
        /** The columns produced by the [InsertOperator]. */
        val COLUMNS: List<ColumnDef<*>> = listOf(
            ColumnDef(Name.ColumnName("tupleId"), Types.Long, false),
            ColumnDef(Name.ColumnName("duration_ms"), Types.Double, false)
        )
    }

    /** Columns produced by [InsertOperator]. */
    override val columns: List<ColumnDef<*>> = COLUMNS

    /**
     * Converts this [InsertOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [InsertOperator]
     */
    @ExperimentalTime
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val columns = this.columns.toTypedArray()
        return flow {
            for (record in this@InsertOperator.records) {
                val timedTupleId = measureTimedValue { this@InsertOperator.entity.insert(record) }
                emit(StandaloneRecord(0L, columns, arrayOf(LongValue(timedTupleId.value!!), DoubleValue(timedTupleId.duration.inWholeMilliseconds))))
            }
        }
    }
}