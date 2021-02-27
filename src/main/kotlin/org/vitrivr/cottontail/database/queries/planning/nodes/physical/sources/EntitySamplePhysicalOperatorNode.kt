package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.statistics.entity.RecordStatistics
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.sources.EntitySampleOperator
import kotlin.math.min

/**
 * A [NullaryPhysicalOperatorNode] that formalizes the random sampling of a physical [Entity] in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class EntitySamplePhysicalOperatorNode(
    override val groupId: Int,
    val entity: Entity,
    override val columns: Array<ColumnDef<*>>,
    override val outputSize: Long,
    val seed: Long = System.currentTimeMillis()
) : NullaryPhysicalOperatorNode() {

    init {
        require(this.outputSize > 0) { "Sample size must be greater than zero for sampling an entity but is $outputSize." }
    }

    override val statistics: RecordStatistics = this.entity.statistics
    override val executable: Boolean = true
    override val canBePartitioned: Boolean = true
    override val cost = Cost(Cost.COST_DISK_ACCESS_READ, Cost.COST_MEMORY_ACCESS) * this.outputSize * this.columns.map {
        this.statistics[it].avgWidth
    }.sum()

    /**
     * Returns a copy of this [EntitySamplePhysicalOperatorNode].
     *
     * @return Copy of this [EntitySamplePhysicalOperatorNode].
     */
    override fun copyWithInputs() = EntitySamplePhysicalOperatorNode(this.groupId, this.entity, this.columns, this.outputSize, this.seed)

    /**
     * Returns a copy of this [EntitySamplePhysicalOperatorNode] and its output.
     *
     * @param inputs The [OperatorNode.Logical] that should act as inputs.
     * @return Copy of this [EntitySamplePhysicalOperatorNode] and its output.
     */
    override fun copyWithOutput(vararg inputs: OperatorNode.Physical): OperatorNode.Physical {
        require(inputs.isEmpty()) { "No input is allowed for nullary operators." }
        val sample = EntitySamplePhysicalOperatorNode(this.groupId, this.entity, this.columns, this.outputSize, this.seed)
        return (this.output?.copyWithOutput(sample) ?: sample)
    }

    /**
     * Partitions this [EntitySamplePhysicalOperatorNode].
     *
     * @param p The number of partitions to create.
     * @return List of [OperatorNode.Physical], each representing a partition of the original tree.
     */
    override fun partition(p: Int): List<NullaryPhysicalOperatorNode> {
        val partitionSize: Long = Math.floorDiv(this.outputSize, p.toLong()) + 1L
        return (0 until p).map {
            val start = it * partitionSize
            val end = min((it + 1L) * partitionSize, this.outputSize)
            EntitySamplePhysicalOperatorNode(this.groupId, this.entity, this.columns, end - start + 1)
        }
    }

    /**
     * Converts this [EntitySamplePhysicalOperatorNode] to a [EntitySampleOperator].
     *
     * @param tx The [TransactionContext] used for execution.
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(tx: TransactionContext, ctx: QueryContext) = EntitySampleOperator(this.groupId, this.entity, this.columns, this.outputSize, this.seed)


    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is EntitySamplePhysicalOperatorNode) return false

        if (entity != other.entity) return false
        if (!columns.contentEquals(other.columns)) return false
        if (outputSize != other.outputSize) return false
        if (seed != other.seed) return false

        return true
    }

    override fun hashCode(): Int {
        var result = entity.hashCode()
        result = 31 * result + columns.contentHashCode()
        result = 31 * result + outputSize.hashCode()
        result = 31 * result + seed.hashCode()
        return result
    }
}