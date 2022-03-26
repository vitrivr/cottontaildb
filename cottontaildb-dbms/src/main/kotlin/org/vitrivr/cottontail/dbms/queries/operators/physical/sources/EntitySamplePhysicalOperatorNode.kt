package org.vitrivr.cottontail.dbms.queries.operators.physical.sources

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.nodes.traits.NotPartitionableTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.column.ColumnTx
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.sources.EntitySampleOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.physical.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.statistics.columns.ValueStatistics

/**
 * A [NullaryPhysicalOperatorNode] that formalizes the random sampling of a physical [Entity] in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 2.6.0
 */
@Suppress("UNCHECKED_CAST")
class EntitySamplePhysicalOperatorNode(override val groupId: Int, val entity: EntityTx, val fetch: List<Pair<Binding.Column, ColumnDef<*>>>, val p: Float, val seed: Long = System.currentTimeMillis()) : NullaryPhysicalOperatorNode() {

    companion object {
        private const val NODE_NAME = "SampleEntity"
    }

    init {
        require(this.p in 0.0f..1.0f) { "Probability p must be between 0.0 and 1.0 but has value $p."}
    }

    /** The name of this [EntityScanPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The physical [ColumnDef] accessed by this [EntitySamplePhysicalOperatorNode]. */
    override val physicalColumns: List<ColumnDef<*>> = this.fetch.map { it.second }

    /** The [ColumnDef] produced by this [EntityScanPhysicalOperatorNode]. */
    override val columns: List<ColumnDef<*>> = this.fetch.map { it.first.column }

    /** The output size of the [EntitySamplePhysicalOperatorNode] is actually limited by the size of the [Entity]s. */
    override val outputSize: Long = (this.entity.count() * this.p).toLong()

    /** [EntitySamplePhysicalOperatorNode] is always executable. */
    override val executable: Boolean = true

    /** [ValueStatistics] are taken from the underlying [Entity]. The query planner uses statistics for [Cost] estimation. */
    override val statistics = Object2ObjectLinkedOpenHashMap<ColumnDef<*>,ValueStatistics<*>>()

    /** The estimated [Cost] incurred by this [EntitySamplePhysicalOperatorNode]. */
    override val cost: Cost

    /** The [EntitySampleOperator] cannot be partitioned. */
    override val traits: Map<TraitType<*>, Trait> = mapOf(NotPartitionableTrait to NotPartitionableTrait)

    /** Initialize entity statistics. */
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
        this.cost = (Cost.DISK_ACCESS_READ + Cost.MEMORY_ACCESS) * this.outputSize * fetchSize
    }

    /**
     * Creates and returns a copy of this [EntityScanPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [EntityScanPhysicalOperatorNode].
     */
    override fun copy() = EntitySamplePhysicalOperatorNode(this.groupId, this.entity, this.fetch, this.p, this.seed)

    /**
     * Converts this [EntitySamplePhysicalOperatorNode] to a [EntitySampleOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): EntitySampleOperator {
        /* Bind all column bindings to context. */
        this.fetch.forEach { it.first.bind(ctx.bindings) }

        /* Generate and return EntitySampleOperator. */
        return EntitySampleOperator(this.groupId, this.entity, this.fetch, this.p, this.seed)
    }

    /** Generates and returns a [String] representation of this [EntitySamplePhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.columns.joinToString(",") { it.name.toString() }}]"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is EntitySamplePhysicalOperatorNode) return false

        if (this.entity != other.entity) return false
        if (this.columns != other.columns) return false
        if (this.outputSize != other.outputSize) return false
        if (this.seed != other.seed) return false

        return true
    }

    override fun hashCode(): Int {
        var result = this.entity.hashCode()
        result = 31 * result + this.columns.hashCode()
        result = 31 * result + this.outputSize.hashCode()
        result = 31 * result + this.seed.hashCode()
        return result
    }
}