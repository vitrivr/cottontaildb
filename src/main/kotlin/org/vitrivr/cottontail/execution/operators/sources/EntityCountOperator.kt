package org.vitrivr.cottontail.execution.operators.sources

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.queries.GroupId
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.database.queries.projection.Projection
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.values.types.Types
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.LongValue

/**
 * An [Operator.SourceOperator] that counts the number of entries in an [Entity] and returns one [Record] with that number.
 *
 * @author Ralph Gasser
 * @version 1.6.0
 */
class EntityCountOperator(groupId: GroupId, val entity: EntityTx, override val binding: BindingContext, val alias: Name.ColumnName? = null) : Operator.SourceOperator(groupId) {

    /** The [ColumnDef] returned by this [EntitySampleOperator]. */
    override val columns: List<ColumnDef<*>> = listOf(
        ColumnDef(alias ?: entity.dbo.name.column(Projection.COUNT.label()), Types.Long)
    )

    /**
     * Converts this [EntityCountOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution.
     * @return [Flow] representing this [EntityCountOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> = flow {
        val record = StandaloneRecord(0L, this@EntityCountOperator.columns.toTypedArray(), arrayOf(LongValue(this@EntityCountOperator.entity.count())))
        this@EntityCountOperator.binding.bindRecord(record) /* Important: Make new record available to binding context. */
        emit(record)
    }
}