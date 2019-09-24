package ch.unibas.dmi.dbis.cottontail.execution.tasks.recordset.merge

import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.execution.tasks.recordset.projection.RecordsetCountProjectionTask
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import com.github.dexecutor.core.task.Task
import com.github.dexecutor.core.task.TaskExecutionException
import java.lang.IllegalArgumentException

/**
 * A [Task] used during query execution. It takes two [Recordset]s as input and creates a new [Recordset] containing the UNION of the two inputs.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class RecordsetUnionTask (val unique: Boolean = false, sizeEstimate: Int = 1000): ExecutionTask("RecordsetUnionTask") {
    /** The estimated cost of this [RecordsetUnionTask] depends linearly on the size estimate. */
    override val cost = 0.001f * sizeEstimate

    /**
     * Executes this [RecordsetCountProjectionTask]
     */
    override fun execute(): Recordset {
        assertBinaryInput()

        val left = this.get(0) ?: throw TaskExecutionException("Recordset UNION could not be executed because left recordset does not exists.")
        val right = this.get(1) ?: throw TaskExecutionException("Recordset UNION could not be executed because right recordset does not exists.")

        try {
            return left.union(right)
        } catch (e: IllegalArgumentException) {
            throw TaskExecutionException("Recordset UNION could not be executed because left recordset does equal right recordset.")
        }
    }
}