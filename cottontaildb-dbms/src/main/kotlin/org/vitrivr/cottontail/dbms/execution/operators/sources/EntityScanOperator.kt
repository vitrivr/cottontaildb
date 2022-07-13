package org.vitrivr.cottontail.dbms.execution.operators.sources

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext

/**
 * An [Operator.SourceOperator] that scans an [Entity] and streams all [Record]s found within.
 *
 * @author Ralph Gasser
 * @version 1.6.0
 */
class EntityScanOperator(groupId: GroupId, val entity: EntityTx, val fetch: List<Pair<Binding.Column, ColumnDef<*>>>, val partitionIndex: Int, val partitions: Int) : Operator.SourceOperator(groupId) {

    companion object {
        /** [Logger] instance used by [EntityScanOperator]. */
        private val LOGGER: Logger = LoggerFactory.getLogger(EntityScanOperator::class.java)
    }

    /** The [ColumnDef] fetched by this [EntityScanOperator]. */
    override val columns: List<ColumnDef<*>> = this.fetch.map { it.first.column }

    /**
     * Converts this [EntityScanOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution
     * @return [Flow] representing this [EntityScanOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> = flow {
        val fetch = this@EntityScanOperator.fetch.map { it.second }.toTypedArray()
        val columns = this@EntityScanOperator.fetch.map { it.first.column }.toTypedArray()
        val partition = this@EntityScanOperator.entity.partitionFor(this@EntityScanOperator.partitionIndex, this@EntityScanOperator.partitions)
        var read = 0
        val cursor = this@EntityScanOperator.entity.cursor(fetch, partition)
        while (cursor.moveNext()) {
            val next = cursor.value() as StandaloneRecord
            for ((i, c) in columns.withIndex()) { /* Replace column designations. */
                next.columns[i] = c
            }
            this@EntityScanOperator.fetch.first().first.context.update(next) /* Important: Make new record available to binding context. */
            emit(next)
            read += 1
            LOGGER.debug("Read $read entries from ${this@EntityScanOperator.entity.dbo.name}.")
        }
        cursor.close()
        LOGGER.debug("Read $read entries from ${this@EntityScanOperator.entity.dbo.name}.")
    }
}