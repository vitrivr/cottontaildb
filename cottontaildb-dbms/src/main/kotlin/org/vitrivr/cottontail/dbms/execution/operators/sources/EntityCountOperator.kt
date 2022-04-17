package org.vitrivr.cottontail.dbms.execution.operators.sources

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext

/**
 * An [Operator.SourceOperator] that counts the number of entries in an [Entity] and returns one [Record] with that number.
 *
 * @author Ralph Gasser
 * @version 1.6.0
 */
class EntityCountOperator(groupId: GroupId, val entity: EntityTx, val out: Binding.Column) : Operator.SourceOperator(groupId) {

    /** The [ColumnDef] returned by this [EntitySampleOperator]. */
    override val columns: List<ColumnDef<*>> = listOf(this.out.column)

    /**
     * Converts this [EntityCountOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution.
     * @return [Flow] representing this [EntityCountOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> = flow {
        val rec = StandaloneRecord(0L, this@EntityCountOperator.columns.toTypedArray(), arrayOf(LongValue(this@EntityCountOperator.entity.count())))
        this@EntityCountOperator.out.context.update(rec)
        emit(rec)
    }
}