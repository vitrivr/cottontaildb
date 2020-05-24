package org.vitrivr.cottontail.execution.tasks.recordset.merge

import com.github.dexecutor.core.task.Task
import com.github.dexecutor.core.task.TaskExecutionException
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionTask
import org.vitrivr.cottontail.execution.tasks.recordset.projection.RecordsetCountProjectionTask
import org.vitrivr.cottontail.model.recordset.Recordset

/**
 * A [Task] used during query execution. It takes two [Recordset]s as input and creates a new [Recordset] containing the UNION of the two inputs.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class RecordsetUnionTask(val unique: Boolean = false, sizeEstimate: Int = 1000) : ExecutionTask("RecordsetUnionTask") {
    /**
     * Executes this [RecordsetCountProjectionTask]
     */
    override fun execute(): Recordset {
        assertBinaryInput()

        val left = this.get(0)
                ?: throw TaskExecutionException("Recordset UNION could not be executed because left recordset does not exists.")
        val right = this.get(1)
                ?: throw TaskExecutionException("Recordset UNION could not be executed because right recordset does not exists.")

        try {
            return left.union(right)
        } catch (e: IllegalArgumentException) {
            throw TaskExecutionException("Recordset UNION could not be executed because left recordset does equal right recordset.")
        }
    }
}