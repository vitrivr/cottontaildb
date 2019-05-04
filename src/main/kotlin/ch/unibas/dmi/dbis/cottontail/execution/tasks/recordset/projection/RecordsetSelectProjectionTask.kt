package ch.unibas.dmi.dbis.cottontail.execution.tasks.recordset.projection

import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.queries.Projection

import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionTask

import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.utilities.name.doesNameMatch

import com.github.dexecutor.core.task.Task
import com.github.dexecutor.core.task.TaskExecutionException

/**
 * A [Task] used during query execution. It takes a single [Recordset] as input and fetches the specified [Column] values from the
 * specified [Entity] in order to addRow these values to the resulting [Recordset].
 *
 * A [Recordset] that undergoes this task will grow in terms of columns it contains. However, it should not affect the number of rows.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class RecordsetSelectProjectionTask (val projection: Projection): ExecutionTask("RecordsetSelectProjectionTask") {

    /** Cost estimate for a [RecordsetSelectProjectionTask]. */
    override val cost = 1.0f

    /**
     * Executes this [RecordsetSelectProjectionTask]
     */
    override fun execute(): Recordset {
        assertUnaryInput()

        /* Get records from parent task. */
        val parent = this.first() ?: throw TaskExecutionException("Projection could not be executed because parent task has failed.")

        /* Find longest, common prefix of all projection sub-clauses. */
        val longestCommonPrefix = findLongestPrefix(this.projection.fields.keys)

        /* Determine columns to rename. */
        val dropIndex = mutableListOf<Int>()
        val renameIndex = mutableListOf<Pair<Int,String>>()
        parent.columns.forEachIndexed { i, c ->
            var match = false
            this.projection.fields.forEach { (k, v) ->
                if (k.doesNameMatch(c.name)) {
                    match = true
                    if (v != null) {
                        renameIndex.add(Pair(i, v))
                    } else if (longestCommonPrefix != "") {
                        renameIndex.add(Pair(i, c.name.replace(longestCommonPrefix, "")))
                    }
                }
            }
            if (!match) dropIndex.add(i)
        }

        /* Create new Recordset with specified columns and return it . */
        return if (dropIndex.size > 0 && renameIndex.size > 0) {
            parent.dropColumnsWithIndex(dropIndex).renameColumnsWithIndex(renameIndex)
        } else if (renameIndex.size > 0) {
            parent.renameColumnsWithIndex(renameIndex)
        } else if (dropIndex.size > 0) {
            parent.dropColumnsWithIndex(dropIndex)
        } else {
            return parent
        }
    }

    /**
     *
     */
    private fun findLongestPrefix(strings: Collection<String>) : String {
        val prefix = Array<String?>(3, { null })
        for (i in 0 until 3) {
            val d = strings.map { it.split('.')[i] }.distinct()
            if (d.size == 1) {
                prefix[i] = d.first()
            } else {
                break
            }
        }
        return prefix.filterNotNull().joinToString (separator = ".", postfix = ".")
    }
}