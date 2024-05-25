package org.vitrivr.cottontail.dbms.execution.operators.sources

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import java.util.*

/**
 * An [Operator.SourceOperator] that samples an [Entity] and streams all [Tuple]s found within.
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
class EntitySampleOperator(groupId: GroupId, private val entity: EntityTx, private val p: Float, private val seed: Long, override val context: QueryContext) : Operator.SourceOperator(groupId) {

    companion object {
        /** [Logger] instance used by [EntitySampleOperator]. */
        private val LOGGER: Logger = LoggerFactory.getLogger(EntitySampleOperator::class.java)
    }

    /** The [ColumnDef] fetched by this [EntitySampleOperator]. */
    override val columns: List<ColumnDef<*>> by lazy { this.entity.listColumns() }

    /**
     * Converts this [EntitySampleOperator] to a [Flow] and returns it.
     *
     * @return [Flow] representing this [EntitySampleOperator].
     */
    override fun toFlow(): Flow<Tuple> = flow {
        val random = SplittableRandom(this@EntitySampleOperator.seed)
        var read = 0
        this@EntitySampleOperator.entity.cursor().use { cursor ->
            while (cursor.moveNext()) {
                if (random.nextDouble(0.0, 1.0) <= this@EntitySampleOperator.p) {
                    emit(cursor.value())
                    read += 1
                }
            }
        }
        LOGGER.debug("Read {} entries from {}.", read, this@EntitySampleOperator.entity.dbo.name)
    }
}