package org.vitrivr.cottontail.dbms.queries.operators.physical.sources

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.column.ColumnTx
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.sources.EntityScanOperator
import org.vitrivr.cottontail.dbms.queries.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.merge.MergePhysicalOperator
import org.vitrivr.cottontail.dbms.statistics.columns.ValueStatistics
import java.lang.Math.floorDiv

/**
 * A [UnaryPhysicalOperatorNode] that formalizes a scan of a physical [Entity] in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 2.5.0
 */
class EntityScanPhysicalOperatorNode(override val groupId: Int,
                                     val entity: EntityTx,
                                     val fetch: List<Pair<Binding.Column, ColumnDef<*>>>,
                                     val partitionIndex: Int = 0,
                                     val partitions: Int = 1) : NullaryPhysicalOperatorNode() {

    companion object {
        private const val NODE_NAME = "ScanEntity"
    }

    /** The name of this [EntityScanPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The physical [ColumnDef] accessed by this [EntityScanPhysicalOperatorNode]. */
    override val physicalColumns: List<ColumnDef<*>> = this.fetch.map { it.second }

    /** The [ColumnDef] produced by this [EntityScanPhysicalOperatorNode]. */
    override val columns: List<ColumnDef<*>> = this.fetch.map { it.first.column }

    /** The number of rows returned by this [EntityScanPhysicalOperatorNode] equals to the number of rows in the [Entity]. */
    override val outputSize = this.entity.count()

    /** [EntityScanPhysicalOperatorNode] is always executable. */
    override val executable: Boolean = true

    /** [EntityScanPhysicalOperatorNode] can always be partitioned. */
    override val canBePartitioned: Boolean = true

    /** [ValueStatistics] are taken from the underlying [Entity]. The query planner uses statistics for [Cost] estimation. */
    override val statistics = Object2ObjectLinkedOpenHashMap<ColumnDef<*>,ValueStatistics<*>>()

    /** The estimated [Cost] of scanning the [Entity]. */
    override val cost: Cost

    /** Initialize entity statistics and cost. */
    init {
        var fetchSize = 0
        for ((binding, physical) in this.fetch) {
            if (!this.statistics.containsKey(binding.column)) {
                this.statistics[binding.column] = (this.entity.context.getTx(this.entity.columnForName(physical.name)) as ColumnTx<*>).statistics() as ValueStatistics<Value>
            }
            fetchSize += if (binding.type == Types.String) {
                this.statistics[binding.column]!!.avgWidth * Char.SIZE_BYTES
            } else {
                binding.type.physicalSize
            }
        }
        this.cost = (Cost.DISK_ACCESS_READ + Cost.MEMORY_ACCESS) * floorDiv(this.outputSize, this.partitions) * fetchSize
    }

    /**
     * Creates and returns a copy of this [EntityScanPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [EntityScanPhysicalOperatorNode].
     */
    override fun copy() = EntityScanPhysicalOperatorNode(this.groupId, this.entity, this.fetch.map { it.first.copy() to it.second })

    /**
     * Propagates the [bind] call to all [Binding.Column] processed by this [EntityScanPhysicalOperatorNode].
     *
     * @param context The new [BindingContext]
     */
    override fun bind(context: BindingContext) {
        this.fetch.forEach { it.first.bind(context) }
    }

    /**
     * Create a partitioned version of this [EntityScanPhysicalOperatorNode].
     *
     * @param partitions The number of partitions.
     * @param p The partition number. If this value is set, then partitioning has already taken place downstream.
     * @return Array of [OperatorNode.Physical]s.
     */
    override fun tryPartition(partitions: Int, p: Int?): Physical? {
        if (p != null) return EntityScanPhysicalOperatorNode(p, this.entity, this.fetch.map { it.first.copy() to it.second }, p, partitions)
        val inbound = (0 until partitions).map { i ->
            EntityScanPhysicalOperatorNode(i, this.entity, this.fetch.map { it.first.copy() to it.second }, i, partitions)
        }
        val merge = MergePhysicalOperator(*inbound.toTypedArray())
        return this.output?.copyWithOutput(merge)
    }

    /**
     * Converts this [EntityScanPhysicalOperatorNode] to a [EntityScanOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext) = EntityScanOperator(this.groupId, this.entity, this.fetch, this.partitionIndex, this.partitions)

    /** Generates and returns a [String] representation of this [EntityScanPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.columns.joinToString(",") { it.name.toString() }}]"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is EntityScanPhysicalOperatorNode) return false

        if (this.entity != other.entity) return false
        if (this.columns != other.columns) return false

        return true
    }

    override fun hashCode(): Int {
        var result = entity.hashCode()
        result = 31 * result + this.columns.hashCode()
        return result
    }
}