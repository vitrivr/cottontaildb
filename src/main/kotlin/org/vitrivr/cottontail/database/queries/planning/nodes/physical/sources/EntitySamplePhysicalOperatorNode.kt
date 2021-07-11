package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
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
 * @version 2.2.0
 */
class EntitySamplePhysicalOperatorNode(
    override val groupId: Int,
    val entity: EntityTx,
    override val columns: Array<ColumnDef<*>>,
    val p: Float,
    val seed: Long = System.currentTimeMillis()
) : NullaryPhysicalOperatorNode() {

    companion object {
        private const val NODE_NAME = "SampleEntity"
    }

    init {
        require(this.p in 0.0f..1.0f) { "Probability p must be between 0.0 and 1.0 but has value $p."}
    }

    /** The name of this [EntityScanPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The output size of the [EntitySamplePhysicalOperatorNode] is actually limited by the size of the [Entity]s. */
    override val outputSize: Long = (this.entity.count() * this.p).toLong()

    /** [EntitySamplePhysicalOperatorNode] is always executable. */
    override val executable: Boolean = true

    /** [EntitySamplePhysicalOperatorNode] can always be partitioned. */
    override val canBePartitioned: Boolean = true

    /** The estimated [Cost] of sampling the [Entity]. */
    override val cost = Cost(Cost.COST_DISK_ACCESS_READ, Cost.COST_MEMORY_ACCESS) * this.outputSize * this.columns.map {
        this.statistics[it].avgWidth
    }.sum()

    /** The [RecordStatistics] is taken from the underlying [Entity]. [RecordStatistics] are used by the query planning for [Cost] estimation. */
    override val statistics: RecordStatistics = this.entity.dbo.statistics

    /**
     * Creates and returns a copy of this [EntityScanPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [EntityScanPhysicalOperatorNode].
     */
    override fun copy() = EntitySamplePhysicalOperatorNode(this.groupId, this.entity, this.columns, this.p, this.seed)

    /**
     * Partitions this [EntitySamplePhysicalOperatorNode].
     *
     * @param p The number of partitions to create.
     * @return List of [OperatorNode.Physical], each representing a partition of the original tree.
     */
    override fun partition(p: Int): List<NullaryPhysicalOperatorNode> {
        throw UnsupportedOperationException("EntitySamplePhysicalNodeExpression cannot be partitioned.")
    }

    /**
     * Converts this [EntitySamplePhysicalOperatorNode] to a [EntitySampleOperator].
     *
     * @param tx The [TransactionContext] used for execution.
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(tx: TransactionContext, ctx: QueryContext) = EntitySampleOperator(this.groupId, this.entity, this.columns, this.p, this.seed)

    /** Generates and returns a [String] representation of this [EntitySamplePhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.columns.joinToString(",") { it.name.toString() }}]"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is EntitySamplePhysicalOperatorNode) return false

        if (this.entity != other.entity) return false
        if (!this.columns.contentEquals(other.columns)) return false
        if (this.outputSize != other.outputSize) return false
        if (this.seed != other.seed) return false

        return true
    }

    override fun hashCode(): Int {
        var result = this.entity.hashCode()
        result = 31 * result + this.columns.contentHashCode()
        result = 31 * result + this.outputSize.hashCode()
        result = 31 * result + this.seed.hashCode()
        return result
    }
}