package org.vitrivr.cottontail.dbms.execution.operators.sources

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.AccessMode
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * An [Operator.SourceOperator] that counts the number of entries in an [Entity] and returns one [Tuple] with that number.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class EntityCountOperator(groupId: GroupId, private val entity: Name.EntityName, private val out: Binding.Column, override val context: QueryContext) : Operator.SourceOperator(groupId) {

    /** The [ColumnDef] returned by this [EntitySampleOperator]. */
    override val columns: List<ColumnDef<*>> = listOf(this.out.column)

    /**
     * Converts this [EntityCountOperator] to a [Flow] and returns it.
     *
     * @return [Flow] representing this [EntityCountOperator]
     */
    override fun toFlow(): Flow<Tuple> = flow {
        val entityTxn = this@EntityCountOperator.context.transaction.entityTx(this@EntityCountOperator.entity, AccessMode.READ)
        emit(StandaloneTuple(0L, this@EntityCountOperator.columns.toTypedArray(), arrayOf(LongValue(entityTxn.count()))))
    }
}