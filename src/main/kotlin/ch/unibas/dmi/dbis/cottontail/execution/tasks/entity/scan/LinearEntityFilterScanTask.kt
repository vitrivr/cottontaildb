package ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.scan

import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.query
import ch.unibas.dmi.dbis.cottontail.database.queries.BooleanPredicate
import ch.unibas.dmi.dbis.cottontail.execution.tasks.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset

import com.github.dexecutor.core.task.Task

/**
 * A [Task] that executes a full table scan on a defined [Entity] using a [BooleanPredicate] as scan.
 * Only returns [Record]s that match the provided [BooleanPredicate].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class LinearEntityFilterScanTask(private val entity: Entity, private val predicate: BooleanPredicate) : ExecutionTask("LinearEntityFilterScanTask[${entity.fqn}][$predicate]") {
    override fun execute(): Recordset = entity.Tx(true).query {
        it.filter(this.predicate)
    } ?: Recordset(predicate.columns.toTypedArray())
}