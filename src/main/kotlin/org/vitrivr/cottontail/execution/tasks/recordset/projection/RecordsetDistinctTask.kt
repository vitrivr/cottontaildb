package org.vitrivr.cottontail.execution.tasks.recordset.projection

import com.github.dexecutor.core.task.Task
import com.github.dexecutor.core.task.TaskExecutionException
import it.unimi.dsi.fastutil.longs.LongOpenHashSet
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionTask
import org.vitrivr.cottontail.model.recordset.Recordset
import org.vitrivr.cottontail.utilities.name.Name

/**
 * A [Task] that makes sure, that a [Recordset] only contains distinct values.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class RecordsetDistinctTask() : ExecutionTask("RecordsetDistinctTask") {
    /**
     * Executes this [RecordsetDistinctTask]
     */
    override fun execute(): Recordset {
        assertUnaryInput()

        /* Get records from parent task. */
        val parent = this.first() ?: throw TaskExecutionException("DISTINCT projection could not be executed because parent task has failed.")
        val result = Recordset(parent.columns, parent.rowCount)

        /* Generate a hash per entry and make sure that hash remains unique. */
        val seen = LongOpenHashSet()
        parent.forEach {r ->
            var hash = 0L
            r.columns.map { c -> hash = (c.hashCode() * hash) + r[c].hashCode() }
            if (!seen.contains(hash)) {
                seen.add(hash)
                result.addRowUnsafe(r.values)
            }
        }

        return result
    }
}