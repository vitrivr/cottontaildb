package org.vitrivr.cottontail.execution.operators.transform

import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.basics.PipelineOperator
import org.vitrivr.cottontail.execution.operators.basics.ProducingOperator
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record

/**
 * An [Operator.PipelineBreaker] that can be used to limit the number of incoming [Record]s.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class LimitOperator(parent: ProducingOperator, context: ExecutionEngine.ExecutionContext, val skip: Long, val limit: Long) : PipelineOperator(parent, context) {

    override val columns: Array<ColumnDef<*>> = this.parent.columns

    override val depleted: Boolean
        get() = this.parent.depleted || (this.returned >= this.limit)

    /** Number of records that have been skipped over. Used to keep track of limit status. */
    private var skipped = 0L

    /** Number of records that have been returned. Used to keep track of limit status. */
    private var returned = 0L

    /**
     *
     */
    override fun getNext(input: Record?): Record? {
        /* First skip over results (this may take a while). */
        if (this.skip > 0 && this.skipped < this.skip) {
            while (this.skipped < this.skip) {
                this.parent.next()
                this.skipped++
            }
        }

        /* Now return a limited number of entries. */
        if (this.returned < this.limit) {
            this.returned++
            return this.parent.next()
        }

        return null
    }
}