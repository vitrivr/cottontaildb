package org.vitrivr.cottontail.execution.operators.sources

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.queries.components.BooleanPredicate
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.basics.OperatorStatus
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record

/**
 * An [AbstractEntityOperator] that scans an [Entity.Index] and streams all [Record]s found within.
 *
 * @author Ralph Gasser
 * @version 1.1.2
 */
class EntityIndexScanOperator(context: ExecutionEngine.ExecutionContext, entity: Entity, columns: Array<ColumnDef<*>>, private val predicate: BooleanPredicate, val indexHint: IndexType) : AbstractEntityOperator(context, entity, columns) {

    override fun toFlow(scope: CoroutineScope): Flow<Record> {
        check(this.status == OperatorStatus.OPEN) { "Cannot convert operator $this to flow because it is in state ${this.status}." }
        val tx = this.context.getTx(this.entity)
        val indexTx = tx.indexes(this.predicate.columns.toTypedArray(), this.indexHint).first()
        return flow {
            indexTx.filter(this@EntityIndexScanOperator.predicate).use { iterator ->
                for (record in iterator) {
                    emit(tx.read(record.tupleId, this@EntityIndexScanOperator.columns))
                }
            }
        }
    }
}