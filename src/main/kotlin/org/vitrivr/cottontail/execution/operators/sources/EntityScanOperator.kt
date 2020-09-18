package org.vitrivr.cottontail.execution.operators.sources

import org.vitrivr.cottontail.database.entity.Entity
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
class EntityScanOperator(context: ExecutionEngine.ExecutionContext, entity: Entity, override val columns: Array<ColumnDef<*>>, private val range: LongRange = 1L..entity.statistics.maxTupleId) : AbstractEntityOperator(context, entity, columns) {
    /** The maximum tuple ID held by the [Entity]. */
    private var nextTupleId = this.range.first

    /** True, if this [EntityScanOperator] is depleted, i.e., won't return any more [Record]s. */
    override val depleted: Boolean
        get() = !this.range.contains(this.nextTupleId)

    override fun getNext(): Record? {
        var record: Record? = null
        var next = this.nextTupleId
        while (this.range.contains(next++) && record == null) {
            record = this.transaction!!.read(next)
        }
        this.nextTupleId = next
        return record
    }
}