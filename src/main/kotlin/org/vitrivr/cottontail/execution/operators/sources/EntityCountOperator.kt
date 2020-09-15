package org.vitrivr.cottontail.execution.operators.sources

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.LongValue

/**
 * An [Operator.SourceOperator] that counts the number of entries in an [Entity] and returns one
 * [Record] with that number.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class EntityCountOperator(context: ExecutionEngine.ExecutionContext, entity: Entity) : AbstractEntityOperator(context, entity, arrayOf(entity.allColumns().first())) {

    /** The [ColumnDef] returned by this [EntitySampleOperator]. */
    override val columns: Array<ColumnDef<*>> = arrayOf(ColumnDef.withAttributes(entity.name.column("count()"), "LONG"))

    /** The number of entries that have been returned from the [Entity]. */
    override var depleted = false
        private set

    override fun getNext(): Record? {
        if (!this.depleted) {
            val record = StandaloneRecord(0L, this.columns, arrayOf(LongValue(this.transaction!!.count())))
            this.depleted = true
            return record
        }
        return null
    }
}