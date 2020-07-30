package org.vitrivr.cottontail.execution.tasks.recordset.filter

import com.github.dexecutor.core.task.Task
import com.github.dexecutor.core.task.TaskExecutionException
import org.vitrivr.cottontail.database.queries.components.BooleanPredicate
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionTask
import org.vitrivr.cottontail.model.recordset.Recordset

/**
 * A [Task] that applies a [BooleanPredicate] on an input [Recordset] and returns the filtered [Recordset].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class RecordsetScanFilterTask(private val predicate: BooleanPredicate) : ExecutionTask("RecordsetScanFilterTask[$predicate]") {
    /**
     * Executes this [RecordsetScanFilterTask]
     */
    override fun execute(): Recordset {
        /* Get records from parent task. */
        val parent = this.first()
                ?: throw TaskExecutionException("${this.id} could not be executed because parent task has failed.")
        return parent.filter(this.predicate)
    }
}