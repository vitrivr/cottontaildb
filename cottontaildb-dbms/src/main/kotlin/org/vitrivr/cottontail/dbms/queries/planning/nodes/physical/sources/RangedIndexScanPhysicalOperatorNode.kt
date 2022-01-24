package org.vitrivr.cottontail.dbms.queries.planning.nodes.physical.sources

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.index.Index
import org.vitrivr.cottontail.dbms.index.IndexTx
import org.vitrivr.cottontail.dbms.queries.QueryContext
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.dbms.queries.planning.nodes.physical.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.dbms.statistics.columns.ValueStatistics
import org.vitrivr.cottontail.dbms.statistics.entity.RecordStatistics
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.sources.IndexScanOperator
import org.vitrivr.cottontail.core.values.types.Value
import java.lang.Math.floorDiv

/**
 * A [NullaryPhysicalOperatorNode] that formalizes a scan of a physical [Index] in Cottontail DB on a given range.
 *
 * @author Ralph Gasser
 * @version 2.4.0
 */
class RangedIndexScanPhysicalOperatorNode(override val groupId: Int, val index: IndexTx, val predicate: Predicate, val fetch: List<Pair<Name.ColumnName, ColumnDef<*>>>, val partitionIndex: Int, val partitions: Int) : NullaryPhysicalOperatorNode() {
    companion object {
        private const val NODE_NAME = "ScanIndex"
    }

    /** The name of this [RangedIndexScanPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME


    /** The [ColumnDef]s accessed by this [RangedIndexScanPhysicalOperatorNode] depends on the [ColumnDef]s produced by the [Index]. */
    override val physicalColumns: List<ColumnDef<*>> = this.fetch.map {
        require(this.index.dbo.produces.contains(it.second)) { "The given column $it is not produced by the selected index ${this.index.dbo}. This is a programmer's error!"}
        it.second
    }

    /** The [ColumnDef]s produced by this [RangedIndexScanPhysicalOperatorNode] depends on the [ColumnDef]s produced by the [Index]. */
    override val columns: List<ColumnDef<*>> = this.fetch.map {
        require(this.index.dbo.produces.contains(it.second)) { "The given column $it is not produec by the selected index ${this.index.dbo}. This is a programmer's error!"}
        it.second.copy(name = it.first)
    }

    /** [RangedIndexScanPhysicalOperatorNode] are always executable. */
    override val executable: Boolean = true

    /** [RangedIndexScanPhysicalOperatorNode] cannot be further. */
    override val canBePartitioned: Boolean = false

    /** The estimated output size of this [IndexScanPhysicalOperatorNode]. */
    override val outputSize = floorDiv(this.index.dbo.parent.statistics.count, this.partitions)

    /** The [RecordStatistics] is taken from the underlying [Entity]. [RecordStatistics] are used by the query planning for [Cost] estimation. */
    override val statistics: RecordStatistics = this.index.dbo.parent.statistics.let { statistics ->
        this.fetch.forEach {
            val column = it.second.copy(it.first)
            if (!statistics.has(column)) {
                statistics[column] = statistics[it.second] as ValueStatistics<Value>
            }
        }
        statistics
    }

    /** Cost estimation for [IndexScanPhysicalOperatorNode]s is delegated to the [Index]. */
    override val cost = (this.index.dbo.cost(this.predicate) / this.partitions)

    init {
        require(this.partitionIndex >= 0) { "The partitionIndex of a ranged index scan must be greater than zero." }
        require(this.partitions > 0) { "The number of partitions for a ranged index scan must be greater than zero." }
    }

    /**
     * Creates and returns a copy of this [RangedIndexScanPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [RangedIndexScanPhysicalOperatorNode].
     */
    override fun copy() = RangedIndexScanPhysicalOperatorNode(this.groupId, this.index, this.predicate, this.fetch, this.partitionIndex, this.partitions)

    /**
     * Converts this [RangedIndexScanPhysicalOperatorNode] to a [IndexScanOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator = IndexScanOperator(this.groupId, this.index, this.predicate, this.fetch, ctx.bindings, this.partitionIndex, this.partitions)

    /**
     * [RangedIndexScanPhysicalOperatorNode] cannot be partitioned.
     */
    override fun partition(p: Int): List<Physical> {
        throw UnsupportedOperationException("RangedIndexScanPhysicalOperatorNode cannot be further partitioned.")
    }

    /** Generates and returns a [String] representation of this [RangedIndexScanPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.index.type},${this.predicate},${this.partitionIndex}/${this.partitions}/]"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is RangedIndexScanPhysicalOperatorNode) return false

        if (this.depth != other.depth) return false
        if (this.predicate != other.predicate) return false
        if (this.partitionIndex != other.partitionIndex) return false
        if (this.partitions != other.partitions) return false

        return true
    }

    override fun hashCode(): Int {
        var result = depth.hashCode()
        result = 31 * result + predicate.hashCode()
        result = 31 * result + partitionIndex.hashCode()
        result = 31 * result + partitions.hashCode()
        return result
    }
}