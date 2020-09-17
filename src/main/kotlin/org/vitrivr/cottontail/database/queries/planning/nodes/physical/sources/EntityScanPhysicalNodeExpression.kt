package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.cost.Costs
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalNodeExpression
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalNodeExpression
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.basics.ProducingOperator
import org.vitrivr.cottontail.execution.operators.sources.EntitySampleOperator
import org.vitrivr.cottontail.execution.operators.sources.EntityScanOperator
import org.vitrivr.cottontail.model.basics.ColumnDef
import java.lang.Math.floorDiv
import kotlin.math.min

/**
 * A [UnaryPhysicalNodeExpression] that formalizes a scan of a physical [Entity] in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class EntityScanPhysicalNodeExpression(val entity: Entity, val columns: Array<ColumnDef<*>> = entity.allColumns().toTypedArray(), val range: LongRange = 1L until entity.statistics.maxTupleId): NullaryPhysicalNodeExpression() {
    init {
        require(this.range.first > 0L) { "Start of a ranged entity scan must be greater than zero." }
    }

    override val outputSize = this.range.last - this.range.first
    override val canBePartitioned: Boolean = true
    override val cost = Cost(this.outputSize * this.columns.size * Costs.DISK_ACCESS_READ, 0.0f, (this.outputSize * this.columns.map { it.physicalSize }.sum()).toFloat())
    override fun copy() = EntityScanPhysicalNodeExpression(this.entity, this.columns, this.range)
    override fun toOperator(context: ExecutionEngine.ExecutionContext): ProducingOperator = EntityScanOperator(context, this.entity, this.columns, this.range)
    override fun partition(p: Int): List<NullaryPhysicalNodeExpression> {
        val partitionSize = floorDiv(this.range.last - this.range.first + 1, p) + 1
        return (0 until p).map {
            val start =  it * partitionSize
            val end = min((it + 1) * partitionSize, this.range.last)
            EntityScanPhysicalNodeExpression(this.entity, this.columns, start until end)
        }
    }
}