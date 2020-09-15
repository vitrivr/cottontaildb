package org.vitrivr.cottontail.execution.operators.sources

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.index.IndexTransaction
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.queries.components.BooleanPredicate
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record

/**
 * An [Operator.SourceOperator] that scans an [Entity] and streams all [Record]s found within.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class EntityIndexScanOperator(context: ExecutionEngine.ExecutionContext, entity: Entity, columns: Array<ColumnDef<*>>, private val predicate: BooleanPredicate, val indexHint: IndexType) : AbstractEntityOperator(context, entity, columns) {

    private var indexTx: IndexTransaction? = null

    override val depleted: Boolean
        get() = TODO("Not yet implemented")

    override fun getNext(): Record? {
        TODO("Not yet implemented")
    }

    override fun prepareOpen() {
        super.prepareOpen()
        this.indexTx = this.transaction!!.indexes(this.predicate.columns.toTypedArray(), this.indexHint).first()
    }
}