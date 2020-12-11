package org.vitrivr.cottontail.execution.operators.sources

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.queries.components.Predicate
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.exceptions.ExecutionException
import org.vitrivr.cottontail.model.basics.Record

/**
 * An [AbstractEntityOperator] that scans an [Entity.Index] and streams all [Record]s found within.
 *
 * @author Ralph Gasser
 * @version 1.1.5
 */
class EntityIndexScanOperator(val index: Index, private val predicate: Predicate) : AbstractEntityOperator(index.parent, index.produces) {
    /**
     * Converts this [EntityIndexScanOperator] to a [Flow] and returns it.
     *
     * @param context The [ExecutionEngine.ExecutionContext] used for execution.
     * @return [Flow] representing this [EntityIndexScanOperator]
     */
    override fun toFlow(context: ExecutionEngine.ExecutionContext): Flow<Record> {
        val tx = context.getTx(this.entity)
        val indexTx = tx.index(this.index.name)
                ?: throw ExecutionException("Could not find desired index ${this.index.name}.")
        return flow {
            indexTx.filter(this@EntityIndexScanOperator.predicate).use { iterator ->
                for (record in iterator) {
                    emit(record)
                }
            }
        }
    }
}