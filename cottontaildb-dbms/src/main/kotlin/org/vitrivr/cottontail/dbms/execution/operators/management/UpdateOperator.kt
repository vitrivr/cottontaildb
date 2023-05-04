package org.vitrivr.cottontail.dbms.execution.operators.management

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * An [Operator.PipelineOperator] used during query execution. Updates all entries in an [Entity]
 * that it receives with the provided [Value].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class UpdateOperator(parent: Operator, private val entity: EntityTx, private val values: List<Pair<ColumnDef<*>, Binding?>>, override val context: QueryContext) : Operator.PipelineOperator(parent) {

    companion object {
        /** The columns produced by the [UpdateOperator]. */
        val COLUMNS: List<ColumnDef<*>> = listOf(
            ColumnDef(Name.ColumnName("updated"), Types.Long, false),
            ColumnDef(Name.ColumnName("duration_ms"), Types.Double, false)
        )
    }

    /** Columns produced by [UpdateOperator]. */
    override val columns: List<ColumnDef<*>> = COLUMNS

    /** [UpdateOperator] does not act as a pipeline breaker. */
    override val breaker: Boolean = false

    /**
     * Converts this [UpdateOperator] to a [Flow] and returns it.
     *
     * @return [Flow] representing this [UpdateOperator]
     */
    override fun toFlow(): Flow<Record> = flow {
        val start = System.currentTimeMillis()
        val incoming = this@UpdateOperator.parent.toFlow()
        val c = this@UpdateOperator.values.map { it.first }.toTypedArray()
        var updated = 0
        with(this@UpdateOperator.context.bindings) {
            incoming.collect { record ->
                with(record) {
                    val v = this@UpdateOperator.values.map { it.second?.getValue() }.toTypedArray()
                    this@UpdateOperator.entity.update(StandaloneRecord(record.tupleId, c, v)) /* Safe, cause tuple IDs are retained for simple queries. */
                    updated += 1
                }
            }
        }
        emit(StandaloneRecord(0L, this@UpdateOperator.columns.toTypedArray(), arrayOf(LongValue(updated), DoubleValue(System.currentTimeMillis() - start))))
    }
}