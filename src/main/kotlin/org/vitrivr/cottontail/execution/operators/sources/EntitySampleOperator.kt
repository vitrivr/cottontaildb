package org.vitrivr.cottontail.execution.operators.sources

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.queries.GroupId
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.model.basics.Record
import java.util.*

/**
 * An [AbstractEntityOperator] that samples an [Entity] and streams all [Record]s found within.
 *
 * @author Ralph Gasser
 * @version 1.5.0
 */
class EntitySampleOperator(groupId: GroupId, entity: EntityTx, columns: Array<ColumnDef<*>>, val p: Float, val seed: Long) : AbstractEntityOperator(groupId, entity, columns) {

    /**
     * Converts this [EntitySampleOperator] to a [Flow] and returns it.
     *
     * @param context The [QueryContext] used for execution.
     * @return [Flow] representing this [EntitySampleOperator].
     */
    override fun toFlow(context: QueryContext): Flow<Record> = flow {
        val random = SplittableRandom(this@EntitySampleOperator.seed)
        for (record in this@EntitySampleOperator.entity.scan(this@EntitySampleOperator.columns)) {
            if (random.nextDouble(0.0, 1.0) <= this@EntitySampleOperator.p) {
                context.bindings.bindRecord(record) /* Important: Make new record available to binding context. */
                emit(record)
            }
        }
    }
}