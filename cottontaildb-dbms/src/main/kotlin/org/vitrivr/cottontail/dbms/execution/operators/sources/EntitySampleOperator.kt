package org.vitrivr.cottontail.dbms.execution.operators.sources

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.flow
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.Binding
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
 * @version 2.0.0
 */
class EntitySampleOperator(groupId: GroupId, private val entity: EntityTx, private val fetch: List<Pair<Binding.Column, ColumnDef<*>>>, private val p: Float, private val seed: Long, override val context: QueryContext) : Operator.SourceOperator(groupId) {

    companion object {
        /** [Logger] instance used by [EntitySampleOperator]. */
        private val LOGGER: Logger = LoggerFactory.getLogger(EntitySampleOperator::class.java)
    }

    /** The [ColumnDef] fetched by this [EntitySampleOperator]. */
    override val columns: List<ColumnDef<*>> = this.fetch.map { it.first.column }

    /**
     * Converts this [EntitySampleOperator] to a [Flow] and returns it.
     *
     * @return [Flow] representing this [EntitySampleOperator].
     */
    override fun toFlow(): Flow<Tuple> = flow {
        val fetch = this@EntitySampleOperator.fetch.map { it.second }.toTypedArray()
        val columns = this@EntitySampleOperator.fetch.map { it.first.column }.toTypedArray()
        val random = SplittableRandom(this@EntitySampleOperator.seed)
        var read = 0
        this@EntitySampleOperator.entity.cursor(fetch).use { cursor ->
            while (cursor.moveNext()) {
                if (random.nextDouble(0.0, 1.0) <= this@EntitySampleOperator.p) {
                    val record = cursor.value()
                    for ((i,c) in columns.withIndex()) { /* Replace column designations. */
                        record.columns[i] = c
                    }
                    read += 1
                    emit(record)
                }
            }
        }
        LOGGER.debug("Read $read entries from ${this@EntitySampleOperator.entity.dbo.name}.")
    }.buffer(1024) /* Buffering up to 1024 records. */
}