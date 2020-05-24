package org.vitrivr.cottontail.execution.tasks.entity.source

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.query
import org.vitrivr.cottontail.execution.tasks.TaskSetupException
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionTask
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.recordset.Recordset
import java.util.*

/**
 * A [ExecutionTask] that samples the entries of a given [Entity].
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
class EntitySampleTask(private val entity: Entity, private val columns: Array<ColumnDef<*>>, val size: Long, seed: Long) : ExecutionTask("EntitySampleTask[${entity.fqn}][${columns.map { it.name }.joinToString(",")}]") {

    /** The [SplittableRandom] used to generate the sample. */
    private val random = SplittableRandom(seed)

    init {
        if (this.size <= 0L) throw TaskSetupException(this, "EntitySampleTask sample size is invalid (size=${this.size}).")
    }

    /**
     * Executes this [EntityLinearScanTask]
     */
    override fun execute(): Recordset = this.entity.Tx(readonly = true, columns = this.columns).query { tx ->
        val recordset = Recordset(this.columns, this.size)
        val maximum = this.entity.statistics.maxTupleId
        for (i in 0 until size) {
            val tupleId = tx.read(this.random.nextLong(2L, maximum))
            recordset.addRow(tupleId)
        }
        recordset
    } ?: Recordset(this.columns, capacity = 0)
}