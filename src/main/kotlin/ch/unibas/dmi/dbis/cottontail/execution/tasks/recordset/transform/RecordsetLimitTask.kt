package ch.unibas.dmi.dbis.cottontail.execution.tasks.recordset.transform

import ch.unibas.dmi.dbis.cottontail.execution.cost.Costs
import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import com.github.dexecutor.core.task.Task
import com.github.dexecutor.core.task.TaskExecutionException

/**
 * A [Task] used during query execution. It takes a [Recordset] as input, skips <i>skip</i> rows and then copies a maximum of <i>limit</i>
 * rows o a new [Recordset], which is then returned. Produces a n x limit [Recordset] at max.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class RecordsetLimitTask (val limit: Long, val skip: Long = 0): ExecutionTask("RecordsetLimitTask") {
    /** The estimated cost of this [RecordsetLimitTask] depends linearly on the number of records that will be returned. */
    override val cost = (limit + skip) + Costs.MEMORY_ACCESS_READ

    override fun execute(): Recordset {
        assertUnaryInput()

        /* Get records from parent task. */
        val parent = this.first() ?: throw TaskExecutionException("Projection could not be executed because parent task has failed.")

        /* Calculate actual bounds. */
        val actualSkip = if (this.skip > 0) {
            this.skip
        } else {
            0
        }
        val actualLimit = if (this.limit > 0) {
            Math.min(this.limit, parent.rowCount.toLong()-actualSkip)
        } else {
            parent.rowCount.toLong()-actualSkip
        }

        /* Limit the recordset. */
        val recordset = Recordset(parent.columns)
        parent.forEachIndexed {i,r ->
            if (i in actualSkip..actualSkip+actualLimit) {
                recordset.addRowUnsafe(r.tupleId, r.values)
            } else if (i > actualLimit) {
                return@forEachIndexed
            }
        }

        /* Make assertion and return recordset. */
        assert(recordset.rowCount <= limit)
        return recordset
    }
}