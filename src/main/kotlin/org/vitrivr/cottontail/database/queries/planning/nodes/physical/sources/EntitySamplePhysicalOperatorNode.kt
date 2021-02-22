package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.sources.EntitySampleOperator
import kotlin.math.min

/**
 * A [NullaryPhysicalOperatorNode] that formalizes the random sampling of a physical [Entity] in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class EntitySamplePhysicalOperatorNode(
    val entity: Entity,
    override val columns: Array<ColumnDef<*>>,
    override val outputSize: Long,
    val seed: Long = System.currentTimeMillis()
) : NullaryPhysicalOperatorNode() {
    init {
        require(this.outputSize > 0) { "Sample size must be greater than zero for sampling an entity." }
    }

    override val executable: Boolean = true
    override val canBePartitioned: Boolean = true
    override val cost = Cost(
        this.columns.map { this.outputSize * it.type.physicalSize * Cost.COST_DISK_ACCESS_READ }.sum(),
        this.outputSize * Cost.COST_MEMORY_ACCESS
    )
    override fun copy() =
        EntitySamplePhysicalOperatorNode(this.entity, this.columns, this.outputSize, this.seed)

    override fun toOperator(tx: TransactionContext, ctx: QueryContext) =
        EntitySampleOperator(this.entity, this.columns, this.outputSize, this.seed)

    override fun partition(p: Int): List<NullaryPhysicalOperatorNode> {
        val partitionSize: Long = Math.floorDiv(this.outputSize, p.toLong()) + 1L
        return (0 until p).map {
            val start = it * partitionSize
            val end = min((it + 1L) * partitionSize, this.outputSize)
            EntitySamplePhysicalOperatorNode(this.entity, this.columns, end - start + 1)
        }
    }

    /**
     * Calculates and returns the digest for this [EntitySamplePhysicalOperatorNode].
     *
     * @return Digest for this [EntitySamplePhysicalOperatorNode]e
     */
    override fun digest(): Long {
        var result = super.digest()
        result = 31L * result + this.entity.hashCode()
        result = 31L * result + this.columns.contentHashCode()
        result = 31L * result + this.outputSize.hashCode()
        result = 31L * result + this.seed.hashCode()
        return result
    }
}