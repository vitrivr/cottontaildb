package org.vitrivr.cottontail.dbms.queries.planning.nodes.physical.sources

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.queries.QueryContext
import org.vitrivr.cottontail.dbms.queries.planning.nodes.physical.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.statistics.columns.ValueStatistics
import org.vitrivr.cottontail.dbms.statistics.entity.RecordStatistics
import org.vitrivr.cottontail.execution.operators.sources.EntitySampleOperator

/**
 * A [NullaryPhysicalOperatorNode] that formalizes the random sampling of a physical [Entity] in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 2.5.0
 */
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

    /** The [RecordStatistics] is taken from the underlying [Entity]. [RecordStatistics] are used by the query planning for [Cost] estimation. */
    override val statistics: RecordStatistics = this.entity.dbo.statistics.let { statistics ->
        this.fetch.forEach {
            val column = it.first.column
            if (!statistics.has(column)) {
                statistics[column] = statistics[it.second] as ValueStatistics<Value>
            }
        }
        statistics
    }

    /** The estimated [Cost] of sampling the [Entity]. */
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
    override fun copy() = EntitySamplePhysicalOperatorNode(this.groupId, this.entity, this.fetch, this.p, this.seed)

    /**
     * Propagates the [bind] call to all [Binding.Column] processed by this [EntitySamplePhysicalOperatorNode].
     *
     * @param context The new [BindingContext]
     */
    override fun bind(context: BindingContext) {
        this.fetch.forEach { it.first.bind(context) }
    }

    /**
     * Converts this [EntitySamplePhysicalOperatorNode] to a [EntitySampleOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext) = EntitySampleOperator(this.groupId, this.entity, this.fetch, this.p, this.seed)

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