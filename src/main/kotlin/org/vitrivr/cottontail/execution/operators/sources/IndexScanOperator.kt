package org.vitrivr.cottontail.execution.operators.sources

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.model.basics.Record

/**
 * An [AbstractEntityOperator] that scans an [Index] and streams all [Record]s found within.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class IndexScanOperator(private val index: Index, private val predicate: Predicate, private val range: LongRange? = null) : AbstractEntityOperator(index.parent, index.produces) {

    /**
     * Converts this [IndexScanOperator] to a [Flow] and returns it.
     *
     * @param context The [TransactionContext] used for execution.
     * @return [Flow] representing this [IndexScanOperator]
     */
    override fun toFlow(context: TransactionContext): Flow<Record> {
        val tx = context.getTx(this.entity) as EntityTx
        val indexTx = context.getTx(tx.indexForName(this.index.name)) as IndexTx
        return if (this.range == null) {
            flow {
                indexTx.filter(this@IndexScanOperator.predicate).forEach {
                    emit(it)
                }
            }
        } else {
            flow {
                indexTx.filterRange(this@IndexScanOperator.predicate, this@IndexScanOperator.range).forEach {
                    emit(it)
                }
            }
        }
    }
}