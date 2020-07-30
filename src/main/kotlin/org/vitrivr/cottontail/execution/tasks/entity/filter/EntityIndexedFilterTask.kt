package org.vitrivr.cottontail.execution.tasks.entity.filter

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.query
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.queries.components.BooleanPredicate
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionTask
import org.vitrivr.cottontail.model.recordset.Recordset

/**
 * A [Task][com.github.dexecutor.core.task.Task] that executes data access through an index on a defined
 * [Entity] using a [BooleanPredicate]. Only returns [Record][org.vitrivr.cottontail.model.basics.Record]s
 * that match the provided [BooleanPredicate].
 *
 * @author Ralph Gasser
 * @version 1.1
 */
class EntityIndexedFilterTask(private val entity: Entity, private val predicate: BooleanPredicate, indexHint: Index) : ExecutionTask("EntityIndexedFilterTask[${entity.fqn}][$predicate]") {
    /** The type of the [Index] that should be used.*/
    private val type = indexHint.type

    /**
     * Executes this [EntityIndexedFilterTask]
     */
    override fun execute(): Recordset = this.entity.Tx(readonly = true, columns = emptyArray()).query { tx ->
        val index = tx.indexes(this.predicate.columns.toTypedArray(), this.type).first()
        val dataset = Recordset(index.produces)
        index.forEach(this.predicate) {
            dataset.addRowUnsafe(it.tupleId, it.values)
        }
        dataset
    } ?: Recordset(this.predicate.columns.toTypedArray(), capacity = 0)
}