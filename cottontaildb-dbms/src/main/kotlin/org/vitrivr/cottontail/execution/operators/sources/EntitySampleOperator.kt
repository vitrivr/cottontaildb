package org.vitrivr.cottontail.execution.operators.sources

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.queries.QueryContext
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import java.util.*

/**
 * An [Operator.SourceOperator] that samples an [Entity] and streams all [Record]s found within.
 *
 * @author Ralph Gasser
 * @version 1.6.0
 */
class EntitySampleOperator(groupId: GroupId, val entity: EntityTx, val fetch: List<Pair<Name.ColumnName, ColumnDef<*>>>, override val binding: BindingContext, val p: Float, val seed: Long) : Operator.SourceOperator(groupId) {

    /** The [ColumnDef] fetched by this [EntitySampleOperator]. */
    override val columns: List<ColumnDef<*>> = this.fetch.map { it.second.copy(name = it.first) }

    /**
     * Converts this [EntitySampleOperator] to a [Flow] and returns it.
     *
     * @param context The [QueryContext] used for execution.
     * @return [Flow] representing this [EntitySampleOperator].
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val fetch = this.fetch.map { it.second }.toTypedArray()
        val columns = this.columns.toTypedArray()
        val values = arrayOfNulls<Value?>(this.columns.size)
        return flow {
            val random = SplittableRandom(this@EntitySampleOperator.seed)
            for (record in this@EntitySampleOperator.entity.scan(fetch)) {
                if (random.nextDouble(0.0, 1.0) <= this@EntitySampleOperator.p) {
                    var i = 0
                    record.forEach { _, v -> values[i++] = v }
                    val r = StandaloneRecord(record.tupleId, columns, values)
                    this@EntitySampleOperator.binding.bindRecord(r) /* Important: Make new record available to binding context. */
                    emit(r)
                }
            }
        }
    }
}