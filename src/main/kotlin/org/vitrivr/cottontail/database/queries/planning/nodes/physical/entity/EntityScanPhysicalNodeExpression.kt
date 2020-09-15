package org.vitrivr.cottontail.database.queries.planning.nodes.physical.entity

import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.cost.Costs
import org.vitrivr.cottontail.execution.ExecutionEngine
import org.vitrivr.cottontail.execution.operators.basics.ProducingOperator
import org.vitrivr.cottontail.execution.operators.sources.EntitySampleOperator
import org.vitrivr.cottontail.execution.operators.sources.EntityScanOperator
import org.vitrivr.cottontail.model.basics.ColumnDef

/**
 * Formalizes a scan of a physical [Entity] in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
sealed class EntityScanPhysicalNodeExpression : AbstractEntityPhysicalNodeExpression() {
    /** An estimation of the cots required to perform this [EntityScanPhysicalNodeExpression]. */
    abstract override val cost: Cost

    /** The [Entity] that is being scanned. */
    abstract val entity: Entity

    /** The [Entity] that is being scanned. */
    abstract val columns: Array<ColumnDef<*>>

    /** The arity of this [EntityScanPhysicalNodeExpression], i.e., the number of parents or inputs allowed. */
    override val inputArity = 0

    /**
     * Simple (full-)table scan.
     */
    data class FullEntityScanPhysicalNodeExpression(override val entity: Entity, override val columns: Array<ColumnDef<*>> = entity.allColumns().toTypedArray()) : EntityScanPhysicalNodeExpression() {
        override val outputSize = this.entity.statistics.rows
        override val cost = Cost(this.entity.statistics.rows * this.columns.size * Costs.DISK_ACCESS_READ, 0.0f, (this.outputSize * this.columns.map { it.physicalSize }.sum()).toFloat())
        override fun copy() = FullEntityScanPhysicalNodeExpression(this.entity, this.columns)
        override fun toOperator(context: ExecutionEngine.ExecutionContext): ProducingOperator = EntityScanOperator(context, this.entity, this.columns)
    }

    /**
     * Ranged table scan i.e. a scan that returns a certain range of the underlying [Entity]
     */
    data class RangedEntityScanPhysicalNodeExpression(override val entity: Entity, override val columns: Array<ColumnDef<*>> = entity.allColumns().toTypedArray(), val start: Long, val end: Long) : EntityScanPhysicalNodeExpression() {
        init {
            require(this.end > this.start) { "Start of a ranged entity scan cannot be greater than its end." }
            require(this.start > 0L) { "Start of a ranged entity scan must be greater than zero." }
        }

        override val outputSize = this.end - this.start
        override val cost = Cost(this.outputSize * this.columns.size * Costs.DISK_ACCESS_READ, 0.0f, (this.outputSize * this.columns.map { it.physicalSize }.sum()).toFloat())
        override fun copy() = RangedEntityScanPhysicalNodeExpression(this.entity, this.columns, this.start, this.end)
        override fun toOperator(context: ExecutionEngine.ExecutionContext): ProducingOperator = EntityScanOperator(context, this.entity, this.columns, this.start until this.end)
    }

    /**
     * Sampled table scan i.e. as scan that returns a random subset of the [Entity].
     */
    data class SampledEntityScanPhysicalNodeExpression(override val entity: Entity, override val columns: Array<ColumnDef<*>> = entity.allColumns().toTypedArray(), val size: Long, val seed: Long = System.currentTimeMillis()) : EntityScanPhysicalNodeExpression() {
        init {
            require(size > 0) { "Sample size must be greater than zero for a sampled scan." }
        }

        override val outputSize = this.size
        override val cost = Cost(this.size * this.columns.size * Costs.DISK_ACCESS_READ, 5 * this.size * Costs.MEMORY_ACCESS_READ, (this.outputSize * this.columns.map { it.physicalSize }.sum()).toFloat())
        override fun copy() = SampledEntityScanPhysicalNodeExpression(this.entity, this.columns, this.size, this.seed)
        override fun toOperator(context: ExecutionEngine.ExecutionContext): ProducingOperator = EntitySampleOperator(context, this.entity, this.columns, this.size, this.seed)
    }
}