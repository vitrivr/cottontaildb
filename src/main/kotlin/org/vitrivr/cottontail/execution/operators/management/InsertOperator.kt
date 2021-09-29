package org.vitrivr.cottontail.execution.operators.management

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.queries.GroupId
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.LongValue
import org.vitrivr.cottontail.model.values.types.Value
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * An [Operator.PipelineOperator] used during query execution. Inserts all incoming entries into an
 * [Entity] that it receives with the provided [Value].
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class InsertOperator(groupId: GroupId, val entity: EntityTx, val records: List<Record>) : Operator.SourceOperator(groupId) {

    companion object {
        /** The columns produced by the [InsertOperator]. */
        val COLUMNS: List<ColumnDef<*>> = listOf(
            ColumnDef(Name.ColumnName("tupleId"), Type.Long, false),
            ColumnDef(Name.ColumnName("duration_ms"), Type.Double, false)
        )
    }

    /** Columns produced by [InsertOperator]. */
    override val columns: List<ColumnDef<*>> = COLUMNS

    /**
     * Converts this [InsertOperator] to a [Flow] and returns it.
     *
     * @param context The [QueryContext] used for execution
     * @return [Flow] representing this [InsertOperator]
     */
    @ExperimentalTime
    override fun toFlow(context: QueryContext): Flow<Record> {
        val columns = this.columns.toTypedArray()
        return flow {
            for (record in this@InsertOperator.records) {
                val timedTupleId = measureTimedValue { this@InsertOperator.entity.insert(record) }
                emit(StandaloneRecord(0L, columns, arrayOf(LongValue(timedTupleId.value!!), DoubleValue(timedTupleId.duration.inWholeMilliseconds))))
            }
        }
    }
}