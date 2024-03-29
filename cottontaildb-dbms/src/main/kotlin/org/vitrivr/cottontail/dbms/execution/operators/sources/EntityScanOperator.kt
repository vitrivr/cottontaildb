package org.vitrivr.cottontail.dbms.execution.operators.sources

import kotlinx.coroutines.flow.Flow
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

/**
 * An [Operator.SourceOperator] that scans an [Entity] and streams all [Tuple]s found within.
 *
 * @author Ralph Gasser
 * @version 1.6.0
 */
class EntityScanOperator(groupId: GroupId, private val entity: EntityTx, private val fetch: List<Binding.Column>, private val partitionIndex: Int, private val partitions: Int, override val context: QueryContext) : Operator.SourceOperator(groupId) {

    companion object {
        /** [Logger] instance used by [EntityScanOperator]. */
        private val LOGGER: Logger = LoggerFactory.getLogger(EntityScanOperator::class.java)
    }

    /** The [ColumnDef] fetched by this [EntityScanOperator]. */
    override val columns: List<ColumnDef<*>> = this.fetch.map { it.column }

    /**
     * Converts this [EntityScanOperator] to a [Flow] and returns it.
     *
     * @return [Flow] representing this [EntityScanOperator]
     */
    override fun toFlow(): Flow<Tuple> = flow {
        val fetch = this@EntityScanOperator.fetch.map {
            it.physical ?: throw IllegalStateException("Cannot execute EntityScanOperator with a non-physical column.")
        }.toTypedArray()
        val rename = this@EntityScanOperator.fetch.map { it.column.name }.toTypedArray()
        val partition = this@EntityScanOperator.entity.partitionFor(this@EntityScanOperator.partitionIndex, this@EntityScanOperator.partitions)
        var read = 0
        this@EntityScanOperator.entity.cursor(fetch, partition, rename).use { cursor ->
            while (cursor.moveNext()) {
                emit(cursor.value())
                read += 1
            }
        }
        LOGGER.debug("Read {} entries from {}.", read, this@EntityScanOperator.entity.dbo.name)
    }
}
