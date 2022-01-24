package org.vitrivr.cottontail.execution.operators.sources

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.queries.projection.Projection
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator

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