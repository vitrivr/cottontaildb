package org.vitrivr.cottontail.dbms.execution.operators.sources

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.queries.context.DefaultQueryContext
import java.util.*

/**
 * An [Operator.SourceOperator] that samples an [Entity] and streams all [Record]s found within.
 *
 * @author Ralph Gasser
 * @version 1.6.0
 */
class EntitySampleOperator(groupId: GroupId, val entity: EntityTx, val fetch: List<Pair<Binding.Column, ColumnDef<*>>>, val p: Float, val seed: Long) : Operator.SourceOperator(groupId) {

    companion object {
        /** [Logger] instance used by [EntitySampleOperator]. */
        private val LOGGER: Logger = LoggerFactory.getLogger(EntitySampleOperator::class.java)
    }

    /** The [ColumnDef] fetched by this [EntitySampleOperator]. */
    override val columns: List<ColumnDef<*>> = this.fetch.map { it.first.column }

    /**
     * Converts this [EntitySampleOperator] to a [Flow] and returns it.
     *
     * @param context The [DefaultQueryContext] used for execution.
     * @return [Flow] representing this [EntitySampleOperator].
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val fetch = this.fetch.map { it.second }.toTypedArray()
        val columns = this.fetch.map { it.first.column }.toTypedArray()
        val random = SplittableRandom(this@EntitySampleOperator.seed)
        return flow {
            val cursor = this@EntitySampleOperator.entity.cursor(fetch)
            var read = 0
            while (cursor.moveNext()) {
                if (random.nextDouble(0.0, 1.0) <= this@EntitySampleOperator.p) {
                    val record = cursor.value()
                    for ((i,c) in columns.withIndex()) { /* Replace column designations. */
                        record.columns[i] = c
                    }
                    this@EntitySampleOperator.fetch.first().first.context.update(record) /* Important: Make new record available to binding context. */
                    emit(record)
                    read += 1
                }
            }
            cursor.close()
            LOGGER.debug("Read $read entries from ${this@EntitySampleOperator.entity.dbo.name}.")
        }
    }
}