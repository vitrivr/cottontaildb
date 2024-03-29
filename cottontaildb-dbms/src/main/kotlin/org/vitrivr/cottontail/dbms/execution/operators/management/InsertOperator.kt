package org.vitrivr.cottontail.dbms.execution.operators.management

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * An [Operator.PipelineOperator] used during query execution. Inserts all incoming entries into an
 * [Entity] that it receives with the provided [Value].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class InsertOperator(groupId: GroupId, private val entity: EntityTx, private val tuples: List<Tuple>, override val context: QueryContext) : Operator.SourceOperator(groupId) {
    companion object {
        /** The columns produced by the [InsertOperator]. */
        val COLUMNS: List<ColumnDef<*>> = listOf(
            ColumnDef(Name.ColumnName.create("tupleId"), Types.Long, false),
            ColumnDef(Name.ColumnName.create("duration_ms"), Types.Double, false)
        )
    }

    /** Columns produced by [InsertOperator]. */
    override val columns: List<ColumnDef<*>> = COLUMNS + this.entity.listColumns()

    /**
     * Converts this [InsertOperator] to a [Flow] and returns it.
     *
     * @return [Flow] representing this [InsertOperator]
     */
    override fun toFlow() = flow {
        val columns = this@InsertOperator.columns.toTypedArray()
        val start = System.currentTimeMillis()
        var lastCreated: Tuple? = null
        for (record in this@InsertOperator.tuples) {
            lastCreated = this@InsertOperator.entity.insert(record)
        }
        if (lastCreated != null) {
            emit(StandaloneTuple(
                0L,
                columns,
                arrayOf<Value?>(LongValue(lastCreated.tupleId), DoubleValue(System.currentTimeMillis() - start)) + Array(lastCreated.size) { lastCreated[it]}
            ))
        }
    }
}