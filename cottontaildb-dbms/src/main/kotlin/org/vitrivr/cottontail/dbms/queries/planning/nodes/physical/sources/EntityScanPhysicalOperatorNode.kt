package org.vitrivr.cottontail.dbms.queries.planning.nodes.physical.sources

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.queries.OperatorNode
import org.vitrivr.cottontail.dbms.queries.QueryContext
import org.vitrivr.cottontail.dbms.queries.planning.nodes.physical.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.statistics.columns.ValueStatistics
import org.vitrivr.cottontail.dbms.statistics.entity.RecordStatistics
import org.vitrivr.cottontail.execution.operators.sources.EntityScanOperator

/**
 * A [UnaryPhysicalOperatorNode] that formalizes a scan of a physical [Entity] in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 2.4.0
 */
class EntityScanPhysicalOperatorNode(override val groupId: Int, val entity: EntityTx, val fetch: List<Pair<Name.ColumnName, ColumnDef<*>>>) : NullaryPhysicalOperatorNode() {

    companion object {
        private const val NODE_NAME = "ScanEntity"
    }

    /** The name of this [EntityScanPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The physical [ColumnDef] accessed by this [EntityScanPhysicalOperatorNode]. */
    override val physicalColumns: List<ColumnDef<*>> = this.fetch.map { it.second }

    /** The [ColumnDef] produced by this [EntityScanPhysicalOperatorNode]. */
    override val columns: List<ColumnDef<*>> = this.fetch.map { it.second.copy(name = it.first) }

    /** The number of rows returned by this [EntityScanPhysicalOperatorNode] equals to the number of rows in the [Entity]. */
    override val outputSize = this.entity.count()

    /** [EntityScanPhysicalOperatorNode] is always executable. */
    override val executable: Boolean = true

    /** [EntityScanPhysicalOperatorNode] can always be partitioned. */
    override val canBePartitioned: Boolean = true

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
    override fun copy() = EntityScanPhysicalOperatorNode(this.groupId, this.entity, this.fetch)

    /**
     * Partitions this [EntityScanPhysicalOperatorNode].
     *
     * @param p The number of partitions to create.
     * @return List of [OperatorNode.Physical], each representing a partition of the original tree.
     */
    override fun partition(p: Int): List<NullaryPhysicalOperatorNode> {
        return (0 until p).map { RangedEntityScanPhysicalOperatorNode(this.groupId, this.entity, this.fetch, it, p) }
    }

    /**
     * Converts this [EntityScanPhysicalOperatorNode] to a [EntityScanOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext) = EntityScanOperator(this.groupId, this.entity, this.fetch, ctx.bindings,0, 1)

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