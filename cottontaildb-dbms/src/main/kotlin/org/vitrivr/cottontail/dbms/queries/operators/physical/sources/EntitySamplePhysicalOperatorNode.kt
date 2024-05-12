package org.vitrivr.cottontail.dbms.queries.operators.physical.sources

import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.nodes.traits.NotPartitionableTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.sources.EntitySampleOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.statistics.estimateTupleSize
import org.vitrivr.cottontail.dbms.statistics.values.ValueStatistics

/**
 * A [NullaryPhysicalOperatorNode] that formalizes the random sampling of a physical [Entity] in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 2.8.0
 */
@Suppress("UNCHECKED_CAST")
class EntitySamplePhysicalOperatorNode(override val groupId: Int, val tx: EntityTx, override val columns: List<Binding.Column>, val p: Float, val seed: Long = System.currentTimeMillis()) : NullaryPhysicalOperatorNode() {

    companion object {
        private const val NODE_NAME = "SampleEntity"
    }

    init {
        require(this.p in 0.0f..1.0f) { "Probability p must be between 0.0 and 1.0 but has value $p."}
        require(this.columns.all { it.physical != null }) { "EntityScanPhysicalOperatorNode can only work with physical columns." }
    }

    /** The name of this [EntityScanPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The output size of the [EntitySamplePhysicalOperatorNode] is actually limited by the size of the [Entity]s. */
    override val outputSize: Long = (this.tx.count() * this.p).toLong()

    /** [ValueStatistics] are taken from the underlying [Entity]. The query planner uses statistics for [Cost] estimation. */
    override val statistics  by lazy {
        this.columns.associate {
            it.column to this.tx.columnForName(it.physical!!.name).newTx(this.tx).statistics() as ValueStatistics<Value>
        }
    }

    /** The estimated [Cost] incurred by this [EntitySamplePhysicalOperatorNode]. */
    override val cost: Cost by lazy {
        (Cost.DISK_ACCESS_READ_SEQUENTIAL + Cost.MEMORY_ACCESS) * this.outputSize * this.statistics.estimateTupleSize()
    }

    /** The [EntitySampleOperator] cannot be partitioned. */
    override val traits: Map<TraitType<*>, Trait> = mapOf(NotPartitionableTrait to NotPartitionableTrait)

    /**
     * Creates and returns a copy of this [EntityScanPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [EntityScanPhysicalOperatorNode].
     */
    override fun copy() = EntitySamplePhysicalOperatorNode(this.groupId, this.tx, this.columns, this.p, this.seed)

    /**
     * An [EntitySamplePhysicalOperatorNode] is always executable
     *
     * @param ctx The [QueryContext] to check.
     * @return True
     */
    override fun canBeExecuted(ctx: QueryContext): Boolean = true

    /**
     * Converts this [EntitySamplePhysicalOperatorNode] to a [EntitySampleOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): EntitySampleOperator = EntitySampleOperator(this.groupId, this.tx, this.columns, this.p, this.seed, ctx)

    /** Generates and returns a [String] representation of this [EntitySamplePhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.columns.joinToString(",") { it.physical!!.name.toString() }}]"

    /**
     * Generates and returns a [Digest] for this [EntitySamplePhysicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest {
        var result = this.tx.dbo.name.hashCode() + 2L
        result += 31L * result + this.p.hashCode()
        result += 31L * result + this.columns.hashCode()
        return result
    }
}