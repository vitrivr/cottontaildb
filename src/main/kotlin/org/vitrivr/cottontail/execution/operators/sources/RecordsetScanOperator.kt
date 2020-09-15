package org.vitrivr.cottontail.execution.operators.sources

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.basics.SourceOperator
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.recordset.Recordset

/**
 * An [Operator.SourceOperator] that scans a [Recordset] and streams all [Record]s found within.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class RecordsetScanOperator(context: ExecutionEngine.ExecutionContext, private val recordset: Recordset, private val range: LongRange = 0L..recordset.rowCount) : SourceOperator(context) {

    /** The [ColumnDef]s produced by this [RecordsetScanOperator]. */
    override val columns: Array<ColumnDef<*>> = this.recordset.columns

    /** True, if this [EntityScanOperator] is depleted, i.e., won't return any more [Record]s. */
    override val operational: Boolean = true

    /** True, if this [EntityScanOperator] is depleted, i.e., won't return any more [Record]s. */
    override val depleted: Boolean
        get() = !this.range.contains(this.nextTupleId)

    /** The maximum tuple ID held by the [Entity]. */
    private var nextTupleId = this.range.first

    override fun getNext(): Record? = this.recordset[this.nextTupleId++]

    override fun prepareOpen() {}

    override fun prepareClose() {}
}