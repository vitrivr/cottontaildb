package org.vitrivr.cottontail.dbms.execution.operators.sources

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.index.basic.IndexTx
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * An [Operator.SourceOperator] that scans an [Index] and streams all [Tuple]s found within.
 *
 * @author Ralph Gasser
 * @version 1.8.0
 */
class IndexScanOperator(
    groupId: GroupId,
    private val index: IndexTx,
    private val predicate: Predicate,
    private val fetch: List<Binding.Column>,
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
        it.column
    }

    /**
     * Converts this [IndexScanOperator] to a [Flow] and returns it.
     *
     * @return [Flow] representing this [IndexScanOperator]
     */
    override fun toFlow(): Flow<Tuple> = flow {
        val columns = this@IndexScanOperator.fetch.map { it.column }.toTypedArray()
        val physical = this@IndexScanOperator.fetch.mapNotNull { it.physical }.toTypedArray()
        var read = 0L
        if (this@IndexScanOperator.partitions == 1) {
            this@IndexScanOperator.index.filter(this@IndexScanOperator.predicate)
        } else {
            val entityTx = this@IndexScanOperator.index.dbo.parent.newTx(this@IndexScanOperator.context)
            this@IndexScanOperator.index.filter(this@IndexScanOperator.predicate, entityTx.partitionFor(this@IndexScanOperator.partitionIndex, this@IndexScanOperator.partitions))
        }.use { cursor ->
            while (cursor.moveNext()) {
                val record = cursor.value()
                emit(StandaloneTuple(record.tupleId, columns, physical.map { record[it] }.toTypedArray()))
                read += 1L
            }
        }
        LOGGER.debug("Read {} entries from {}.", read, this@IndexScanOperator.index.dbo.name)
    }
}