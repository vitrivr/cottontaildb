package org.vitrivr.cottontail.execution.operators.sources

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.exceptions.OperatorSetupException
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Record
import java.util.*

/**
 * An [Operator.SourceOperator] that samples an [Entity] and streams all [Record]s found within.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class EntitySampleOperator(context: ExecutionEngine.ExecutionContext, entity: Entity, columns: Array<ColumnDef<*>>, val size: Long, seed: Long) : AbstractEntityOperator(context, entity, columns) {
    /** The [SplittableRandom] used to generate the sample. */
    private val random = SplittableRandom(seed)

    /** The number of entries that have been returned from the [Entity]. */
    private var returned = 0L

    init {
        if (this.size <= 0L) throw OperatorSetupException(this, "EntitySampleOperator sample size is invalid (size=${this.size}).")
    }

    /** True, if this [EntityScanOperator] is depleted, i.e., won't return any more [Record]s. */
    override val depleted: Boolean
        get() = this.returned <= this.size

    override fun getNext(): Record? {
        var record: Record? = null
        while (record == null) {
            val next = this.random.nextLong(this.transaction!!.maxTupleId())
            record = this.transaction!!.read(next)
        }
        return record
    }
}