package ch.unibas.dmi.dbis.cottontail.execution.tasks.entity.boolean

import ch.unibas.dmi.dbis.cottontail.database.entity.Entity
import ch.unibas.dmi.dbis.cottontail.database.general.query
import ch.unibas.dmi.dbis.cottontail.database.index.Index
import ch.unibas.dmi.dbis.cottontail.database.queries.BooleanPredicate
import ch.unibas.dmi.dbis.cottontail.execution.tasks.basics.ExecutionTask
import ch.unibas.dmi.dbis.cottontail.model.recordset.Recordset

/**
 * A [Task] that executes data access through an index on a defined [Entity] using a [BooleanPredicate]. Only returns [Record]s that match the provided [BooleanPredicate].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
internal class EntityIndexedFilterTask(private val entity: Entity, private val predicate: BooleanPredicate, indexHint: Index) : ExecutionTask("EntityIndexedFilterTask[${entity.fqn}][$predicate]") {
    /** The cost of this [EntityLinearScanFilterTask] depends on whether or not an [Index] can be employed. */
    override val cost = indexHint.cost(this.predicate)

    /** The type of the [Index] that should be used.*/
    private val type = indexHint.type

    /**
     * Executes this [EntityIndexedFilterTask]
     */
    override fun execute(): Recordset = this.entity.Tx(readonly = true, columns = this.predicate.columns.toTypedArray()).query {tx ->
        val columns = this.predicate.columns.toTypedArray()
        val dataset = Recordset(columns)
        tx.indexes(columns, this.type).first().filter(this.predicate).forEach {
            dataset.addRow(it.tupleId, tx.read(it.tupleId).values)
        }
        dataset
    } ?: Recordset(this.predicate.columns.toTypedArray())
}