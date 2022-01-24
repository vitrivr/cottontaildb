package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sources

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.statistics.columns.ValueStatistics
import org.vitrivr.cottontail.database.statistics.entity.RecordStatistics
import org.vitrivr.cottontail.execution.operators.sources.EntityScanOperator
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.values.types.Types
import org.vitrivr.cottontail.model.values.types.Value
import java.lang.Math.floorDiv

/**
 * A [NullaryPhysicalOperatorNode] that formalizes a scan of a physical [Entity] in Cottontail DB on a given range.
 *
 * @author Ralph Gasser
 * @version 2.4.0
 */
class RangedEntityScanPhysicalOperatorNode(override val groupId: Int, val entity: EntityTx, val fetch: List<Pair<Name.ColumnName,ColumnDef<*>>>, val partitionIndex: Int, val partitions: Int) : NullaryPhysicalOperatorNode() {


    companion object {
        private const val NODE_NAME = "ScanEntity"
    }

    /** The name of this [RangedEntityScanPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The physical [ColumnDef] accessed by this [RangedEntityScanPhysicalOperatorNode]. */
    override val physicalColumns: List<ColumnDef<*>> = this.fetch.map { it.second }

    /** The [ColumnDef] produced by this [RangedEntityScanPhysicalOperatorNode]. */
    override val columns: List<ColumnDef<*>> = this.fetch.map { it.second.copy(name = it.first) }

    /** The number of rows returned by this [RangedEntityScanPhysicalOperatorNode] equals to the number of rows in the [Entity]. */
    override val outputSize: Long = floorDiv(this.entity.count(), this.partitions.toLong())

    /** The [RecordStatistics] is taken from the underlying [Entity]. [RecordStatistics] are used by the query planning for [Cost] estimation. */
    override val statistics: RecordStatistics = this.entity.snapshot.statistics.let { statistics ->
        this.fetch.forEach {
            val column = it.second.copy(it.first)
            if (!statistics.has(column)) {
                statistics[column] = statistics[it.second] as ValueStatistics<Value>
            }
        }
        statistics
    }

    /** [RangedEntityScanPhysicalOperatorNode] is always executable. */
    override val executable: Boolean = true

    /** [RangedEntityScanPhysicalOperatorNode] cannot be partitioned any further. */
    override val canBePartitioned: Boolean = false

    override val cost = Cost(Cost.COST_DISK_ACCESS_READ, Cost.COST_MEMORY_ACCESS) * this.outputSize * this.fetch.sumOf {
        if (it.second.type == Types.String) {
            this.statistics[it.second].avgWidth * Char.SIZE_BYTES
        } else {
            it.second.type.physicalSize
        }
    }

    init {
        require(this.partitions >= 1) { "A range entity scan requires at least one partition in order to be valid." }
        require(this.partitionIndex < this.partitions) { "The partition index must be smaller than the overall number of partitions." }
    }

    /**
     * Creates and returns a copy of this [RangedEntityScanPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [RangedEntityScanPhysicalOperatorNode].
     */
    override fun copy() = RangedEntityScanPhysicalOperatorNode(this.groupId, this.entity, this.fetch, this.partitionIndex, this.partitions)

    /**
     * [RangedIndexScanPhysicalOperatorNode] cannot be partitioned.
     */
    override fun partition(p: Int): List<Physical> {
        throw UnsupportedOperationException("RangedEntityScanPhysicalOperatorNode cannot be further partitioned.")
    }

    /**
     * Converts this [RangedEntityScanPhysicalOperatorNode] to a [EntityScanOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext) = EntityScanOperator(this.groupId, this.entity, this.fetch, ctx.bindings, this.partitionIndex, this.partitions)

    /** Generates and returns a [String] representation of this [RangedEntityScanPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.columns.joinToString(",") { it.name.toString() }},${this.partitionIndex}/${this.partitions}]"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is RangedEntityScanPhysicalOperatorNode) return false

        if (this.entity != other.entity) return false
        if (this.columns != other.columns) return false
        if (this.partitionIndex != other.partitionIndex) return false
        if (this.partitions != other.partitions) return false

        return true
    }

    override fun hashCode(): Int {
        var result = entity.hashCode()
        result = 31 * result + columns.hashCode()
        result = 31 * result + partitionIndex.hashCode()
        result = 31 * result + partitions.hashCode()
        return result
    }
}