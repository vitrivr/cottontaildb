package ch.unibas.dmi.dbis.cottontail.execution.tasks.recordset.projection

import ch.unibas.dmi.dbis.cottontail.model.basics.ColumnDef
import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.begin
import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset
import ch.unibas.dmi.dbis.cottontail.model.values.Value

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
internal class RecordsetSelectProjectionTask (
    private val entity: Entity,
    private val columns: Array<ColumnDef<*>>,
    private val keepUpstream: Boolean = false
): ExecutionTask("RecordsetSelectProjectionTask[${entity.fqn}]${columns.joinToString(separator = "") { "[${it.name}]" }}") {

    /** Cost estimate for a [RecordsetSelectProjectionTask] is (cols * rows * 0.1). */
    override val cost = (columns.size * entity.statistics.rows * 0.1).toFloat()

    /**
     * Executes this [RecordsetSelectProjectionTask]
     */
    override fun execute(): Recordset {
        assertUnaryInput()

        /* Get records from parent task. */
        val parent = this.first() ?: throw TaskExecutionException("Projection could not be executed because parent task has failed.")

        /*
         * Specifies the columns that are already contained in the parent incoming Recordset and those that require a lookup. Also decides whether upstream fields should be retained.
         */
        val all = if (this.keepUpstream) {
            this.columns + parent.columns.filter { !this.columns.contains(it) }.toTypedArray()
        } else {
            this.columns
        }
        val common = all.filter { parent.columns.contains(it) }.toTypedArray()
        val lookup = all.filter { !common.contains(it) }.toTypedArray()


        /* Create new Recordset with specified columns . */
        val recordset = Recordset(all)

        /* Make lookup if necessary, otherwise just drop columns that are not required. */
        if (lookup.isNotEmpty()) {
            this.entity.Tx(readonly = true, columns = lookup).begin {tx ->
                parent.forEach {rec ->
                    val read = tx.read(rec.tupleId)
                    val values: Array<Value<*>?> = all.map {
                        when {
                            read.has(it) -> read[it]
                            else -> parent[rec.tupleId]?.get(it)
                        }
                    }.toTypedArray()
                    recordset.addRow(rec.tupleId, values)
                }
                true
            }
        } else {
            parent.forEach {rec ->
                val values: Array<Value<*>?> = common.map { rec[it] }.toTypedArray()
                recordset.addRow(rec.tupleId, values)
            }
        }

        /* Return new Recordset. */
        return recordset
    }
}