package org.vitrivr.cottontail.dbms.execution.operators.management

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.predicates.FilterOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * An [Operator.MergingPipelineOperator] used during query execution. Deletes all entries in an
 * [Entity] that it receives.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class DeleteOperator(parent: Operator, private val entity: EntityTx, override val context: QueryContext) : Operator.PipelineOperator(parent) {
    companion object {
        /** The columns produced by the [DeleteOperator]. */
        val COLUMNS: List<ColumnDef<*>> = listOf(
            ColumnDef(Name.ColumnName("deleted"), Types.Long, false),
            ColumnDef(Name.ColumnName("duration_ms"), Types.Double, false)
        )
    }

    /** Columns produced by the [DeleteOperator]. */
    override val columns: List<ColumnDef<*>> = COLUMNS

    /** [DeleteOperator] does not act as a pipeline breaker. */
    override val breaker: Boolean = false

    /**
     * Converts this [FilterOperator] to a [Flow] and returns it.
     *
     * @return [Flow] representing this [FilterOperator]
     */
    override fun toFlow(): Flow<Record> = flow {
        var deleted = 0L
        val parent = this@DeleteOperator.parent.toFlow()
        val columns = this@DeleteOperator.columns.toTypedArray()
        val start = System.currentTimeMillis()
        parent.collect {
            this@DeleteOperator.entity.delete(it.tupleId) /* Safe, cause tuple IDs are retained for simple queries. */
            deleted += 1
        }
        emit(StandaloneRecord(0L, columns, arrayOf(LongValue(deleted), DoubleValue(System.currentTimeMillis()-start))))
    }
}