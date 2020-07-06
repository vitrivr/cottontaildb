package org.vitrivr.cottontail.execution.tasks.entity.source

import it.unimi.dsi.fastutil.longs.LongOpenHashSet
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.general.query
import org.vitrivr.cottontail.execution.tasks.TaskSetupException
import org.vitrivr.cottontail.execution.tasks.basics.ExecutionTask
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.recordset.Recordset
import java.lang.Long.min
import java.util.*

/**
 * A [ExecutionTask] that samples the entries of a given [Entity].
 *
 * @author Ralph Gasser
 * @version 1.0.2
 */
class EntitySampleTask(private val entity: Entity, private val columns: Array<ColumnDef<*>>, val size: Long, seed: Long) : ExecutionTask("EntitySampleTask[${columns.map { it.name }.joinToString(",")}]") {

    /** The [SplittableRandom] used to generate the sample. */
    private val random = SplittableRandom(seed)

    init {
        if (this.size <= 0L) throw TaskSetupException(this, "EntitySampleTask sample size is invalid (size=${this.size}).")
    }

    /**
     * Executes this [EntityLinearScanTask]
     */
    override fun execute(): Recordset = this.entity.Tx(readonly = true, columns = this.columns).query { tx ->
        val maximum = this.entity.statistics.maxTupleId
        val count = this.entity.statistics.rows
        if (maximum > 2L) {
            val used = LongOpenHashSet()
            val recordset = Recordset(this.columns, this.size)
            while (used.size < min(count, this.size)) {
                val tupleId = this.random.nextLong(2L, maximum)
                if (used.add(tupleId)) {
                    recordset.addRow(tx.read(tupleId))
                }
            }
            recordset
        } else {
            Recordset(this.columns, 0)
        }
    } ?: Recordset(this.columns, 0)
}