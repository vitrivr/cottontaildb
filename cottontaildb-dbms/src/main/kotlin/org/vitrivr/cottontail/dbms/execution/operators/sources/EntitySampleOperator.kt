package org.vitrivr.cottontail.dbms.execution.operators.sources

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.TransactionContext
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.QueryContext
import java.util.*

/**
 * An [Operator.SourceOperator] that samples an [Entity] and streams all [Record]s found within.
 *
 * @author Ralph Gasser
 * @version 1.6.0
 */
class EntitySampleOperator(groupId: GroupId, val entity: EntityTx, val fetch: List<Pair<Binding.Column, ColumnDef<*>>>, val p: Float, val seed: Long) : Operator.SourceOperator(groupId) {

    /** The [ColumnDef] fetched by this [EntitySampleOperator]. */
    override val columns: List<ColumnDef<*>> = this.fetch.map { it.first.column }

    /**
     * Converts this [EntitySampleOperator] to a [Flow] and returns it.
     *
     * @param context The [QueryContext] used for execution.
     * @return [Flow] representing this [EntitySampleOperator].
     */
    override fun toFlow(context: TransactionContext): Flow<Record> = flow {
        val fetch = this@EntitySampleOperator.fetch.map { it.second }.toTypedArray()
        val columns = this@EntitySampleOperator.fetch.map { it.first.column }.toTypedArray()
        val values = arrayOfNulls<Value>(columns.size)
        val random = SplittableRandom(this@EntitySampleOperator.seed)
        for (record in this@EntitySampleOperator.entity.scan(fetch)) {
            if (random.nextDouble(0.0, 1.0) <= this@EntitySampleOperator.p) {
                for (i in values.indices) values[i] = record[i]
                val rec = StandaloneRecord(record.tupleId, columns, values)
                this@EntitySampleOperator.fetch.first().first.context.update(rec)
                emit(rec)
            }
        }
    }
}