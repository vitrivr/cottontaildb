package org.vitrivr.cottontail.dbms.execution.operators.sources

import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.channelFlow
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.index.basic.IndexTx
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * An [Operator.SourceOperator] that scans an [Index] and streams all [Record]s found within.
 *
 * @author Ralph Gasser
 * @version 1.7.0
 */
class IndexScanOperator(
    groupId: GroupId,
    private val index: IndexTx,
    private val predicate: Predicate,
    private val fetch: List<Pair<Binding.Column, ColumnDef<*>>>,
    private val partitionIndex: Int = 0,
    private val partitions: Int = 1,
    override val context: QueryContext
) : Operator.SourceOperator(groupId) {

    companion object {
        /** [Logger] instance used by [IndexScanOperator]. */
        private val LOGGER: Logger = LoggerFactory.getLogger(IndexScanOperator::class.java)
    }

    /** The [ColumnDef] produced by this [IndexScanOperator]. */
    override val columns: List<ColumnDef<*>> = this.fetch.map {
        require(this.index.columnsFor(this.predicate).contains(it.second)) { "The given column $it is not produced by the selected index ${this.index.dbo}. This is a programmer's error!"}
        it.first.column
    }

    /**
     * Converts this [IndexScanOperator] to a [Flow] and returns it.
     *
     * @return [Flow] representing this [IndexScanOperator]
     */
    override fun toFlow(): Flow<Record> = channelFlow {
        val columns = this@IndexScanOperator.fetch.map { it.first.column }.toTypedArray()
        var read = 0
        if (this@IndexScanOperator.partitions == 1) {
            this@IndexScanOperator.index.filter(this@IndexScanOperator.predicate)
        } else {
            val entityTx = this@IndexScanOperator.index.dbo.parent.newTx(this@IndexScanOperator.context)
            this@IndexScanOperator.index.filter(this@IndexScanOperator.predicate, entityTx.partitionFor(this@IndexScanOperator.partitionIndex, this@IndexScanOperator.partitions))
        }.use { cursor ->
            while (cursor.moveNext()) {
                val record = cursor.value() as StandaloneRecord
                for ((i, c) in columns.withIndex()) {
                    record.columns[i] = c
                }
                send(record)
                read += 1
            }
        }
        LOGGER.debug("Read $read entries from ${this@IndexScanOperator.index.dbo.name}.")
    }.buffer(1000, BufferOverflow.SUSPEND)
}