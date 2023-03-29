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
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * An [Operator.SourceOperator] that scans an [Entity] and streams all [Record]s found within.
 *
 * @author Ralph Gasser
 * @version 1.6.0
 */
class EntityScanOperator(groupId: GroupId, private val entity: EntityTx, private val fetch: List<Pair<Binding.Column, ColumnDef<*>>>, private val partitionIndex: Int, private val partitions: Int, override val context: QueryContext) : Operator.SourceOperator(groupId) {

    companion object {
        /** [Logger] instance used by [EntityScanOperator]. */
        private val LOGGER: Logger = LoggerFactory.getLogger(EntityScanOperator::class.java)
    }

    /** The [ColumnDef] fetched by this [EntityScanOperator]. */
    override val columns: List<ColumnDef<*>> = this.fetch.map { it.first.column }

    /**
     * Converts this [EntityScanOperator] to a [Flow] and returns it.
     *
     * @return [Flow] representing this [EntityScanOperator]
     */
    override fun toFlow(): Flow<Record> = flow {
        val fetch = this@EntityScanOperator.fetch.map { it.second }.toTypedArray()
        val columns = this@EntityScanOperator.fetch.map { it.first.column }.toTypedArray()
        val partition = this@EntityScanOperator.entity.partitionFor(this@EntityScanOperator.partitionIndex, this@EntityScanOperator.partitions)
        var read = 0
        this@EntityScanOperator.entity.cursor(fetch, partition).use { cursor ->
            while (cursor.moveNext()) {
                val record = cursor.value() as StandaloneRecord
                emit(StandaloneRecord(record.tupleId, columns, record.values))
                read += 1
            }
        }
        LOGGER.debug("Read $read entries from ${this@EntityScanOperator.entity.dbo.name}.")
    }
}