package ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.scan

import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.query
import ch.unibas.dmi.dbis.cottontail.database.index.Index
import ch.unibas.dmi.dbis.cottontail.database.queries.BooleanPredicate
import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset

import com.github.dexecutor.core.task.Task

/**
 * A [Task] that executes a full table scan on a defined [Entity] using a [BooleanPredicate] as scan.
 * Only returns [Record]s that match the provided [BooleanPredicate].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class LinearEntityFilterScanTask(private val entity: Entity, private val predicate: BooleanPredicate, indexHint: Index? = null) : ExecutionTask("LinearEntityFilterScanTask[${entity.fqn}][$predicate]") {

    /** The cost of this [LinearEntityFilterScanTask] depends on whether or not an [Index] can be employed. */
    override val cost = if (indexHint != null && indexHint.canProcess(this.predicate)) {
        indexHint.cost(predicate)
    } else {
        (entity.statistics.columns * predicate.operations).toFloat()
    }

    /**
     * Executes this [LinearEntityFilterScanTask]
     */
    override fun execute(): Recordset = this.entity.Tx(readonly = true, columns = this.predicate.columns.toTypedArray()).query {
        it.filter(this.predicate)
    } ?: Recordset(this.predicate.columns.toTypedArray())
}