package org.vitrivr.cottontail.dbms.queries.operators.physical.sources

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.sources.EntityScanOperator
import org.vitrivr.cottontail.dbms.queries.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.merge.MergePhysicalOperator
import org.vitrivr.cottontail.dbms.statistics.columns.ValueStatistics
import org.vitrivr.cottontail.dbms.statistics.entity.RecordStatistics

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

    /** The [RecordStatistics] is taken from the underlying [Entity]. [RecordStatistics] are used by the query planning for [Cost] estimation. */
    override val statistics: RecordStatistics = this.entity.snapshot.statistics.let { statistics ->
        this.fetch.forEach {
            val column = it.first.column
            if (!statistics.has(column)) {
                statistics[column] = statistics[it.second]  as ValueStatistics<Value>
            }
        }
        statistics
    }

    /** The estimated [Cost] of scanning the [Entity]. */
    override val cost = Cost(Cost.COST_DISK_ACCESS_READ, Cost.COST_MEMORY_ACCESS) * this.outputSize * this.fetch.sumOf {
        if (it.second.type == Types.String) {
            this.statistics[it.second].avgWidth * Char.SIZE_BYTES
        } else {
            it.second.type.physicalSize
        }
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
        if (p != null) return EntityScanPhysicalOperatorNode(p, this.entity, this.fetch, p, partitions)
        val inbound = (0 until partitions).map {
            EntityScanPhysicalOperatorNode(it, this.entity, this.fetch, it, partitions)
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