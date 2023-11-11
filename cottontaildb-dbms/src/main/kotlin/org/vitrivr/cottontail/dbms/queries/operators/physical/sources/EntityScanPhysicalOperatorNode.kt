package org.vitrivr.cottontail.dbms.queries.operators.physical.sources

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.MissingTuple
import org.vitrivr.cottontail.core.queries.nodes.traits.NotPartitionableTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.planning.cost.CostPolicy
import org.vitrivr.cottontail.core.values.Value
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.operators.sources.EntitySampleOperator
import org.vitrivr.cottontail.dbms.execution.operators.sources.EntityScanOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.merge.MergePhysicalOperatorNode
import org.vitrivr.cottontail.dbms.statistics.defaultStatistics
import org.vitrivr.cottontail.dbms.statistics.estimateTupleSize
import org.vitrivr.cottontail.dbms.statistics.values.ValueStatistics

/**
 * A [UnaryPhysicalOperatorNode] that formalizes a scan of a physical [Entity] in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 2.7.1
 */
@Suppress("UNCHECKED_CAST")
class EntityScanPhysicalOperatorNode(
    override val groupId: Int,
    override val context: QueryContext,
    val entity: Name.EntityName,
    val fetch: List<Pair<Binding.Column, ColumnDef<*>>>,
    val partitionIndex: Int = 0,
    val partitions: Int = 1
) : NullaryPhysicalOperatorNode() {

    companion object {
        private const val NODE_NAME = "ScanEntity"
    }

    /** The name of this [EntityScanPhysicalOperatorNode]. */
    override val name: String = NODE_NAME

    /** The physical [ColumnDef] accessed by this [EntityScanPhysicalOperatorNode]. */
    override val physicalColumns: List<ColumnDef<*>> by lazy {
        this.fetch.map { it.second }
    }

    /** The [ColumnDef] produced by this [EntityScanPhysicalOperatorNode]. */
    override val columns: List<ColumnDef<*>> by lazy {
        this.fetch.map { it.first.column }
    }

    /** The number of rows returned by this [EntityScanPhysicalOperatorNode] equals to the number of rows in the [Entity]. */
    override val outputSize by lazy {
        this.context.transaction.manager.statistics[this@EntityScanPhysicalOperatorNode.entity]?.count() ?: 0L
    }

    /** [ValueStatistics] are taken from the underlying [Entity]. The query planner uses statistics for [Cost] estimation. */
    override val statistics by lazy {
        this.fetch.associate {
            it.first.column to ((this.context.transaction.manager.statistics[it.second.name] ?: it.second.type.defaultStatistics<Value>()) as ValueStatistics<Value>)
        }
    }

    /** The estimated [Cost] incurred by scanning the [Entity]. */
    override val cost: Cost by lazy {
        (Cost.DISK_ACCESS_READ_SEQUENTIAL + Cost.MEMORY_ACCESS) * this.outputSize * this.statistics.estimateTupleSize()
    }

    /** The [EntitySampleOperator] cannot be partitioned. */
    override val traits: Map<TraitType<*>, Trait> by lazy {
        if (this.partitions > 1) {
            mapOf(NotPartitionableTrait to NotPartitionableTrait)
        } else {
            emptyMap()
        }
    }

    /**
     * Creates and returns a copy of this [EntityScanPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [EntityScanPhysicalOperatorNode].
     */
    override fun copy() = EntityScanPhysicalOperatorNode(this.groupId, this.context, this.entity, this.fetch.map { it.first.copy() to it.second }, this.partitionIndex, this.partitions)

    /**
     * An [EntityScanPhysicalOperatorNode] is always executable
     *
     * @param ctx The [QueryContext] to check.
     * @return True
     */
    override fun canBeExecuted(ctx: QueryContext): Boolean = true

    /**
     * [EntityScanPhysicalOperatorNode] can be partitioned simply by creating the desired number of partitions and merging the output using the [MergePhysicalOperatorNode]
     *
     * @param ctx The [CostPolicy] to use when determining the optimal number of partitions.
     * @param max The maximum number of partitions to create.
     * @return [OperatorNode.Physical] operator at the based of the new query plan.
     */
    override fun tryPartition(ctx: QueryContext, max: Int): Physical? {
        val partitions = with(ctx.bindings) {
            with(MissingTuple) {
                ctx.costPolicy.parallelisation(this@EntityScanPhysicalOperatorNode.parallelizableCost, this@EntityScanPhysicalOperatorNode.totalCost, max)
            }
        }
        if (partitions <= 1) return null
        val inbound = (0 until partitions).map { this.partition(partitions, it) }
        val merge = MergePhysicalOperatorNode(*inbound.toTypedArray())
        return this.output?.copyWithOutput(merge)
    }

    /**
     * Generates a partitioned version of this [EntityScanPhysicalOperatorNode].
     *
     * @param partitions The total number of partitions.
     * @param p The partition index.
     * @return Partitioned [EntityScanPhysicalOperatorNode]
     */
    override fun partition(partitions: Int, p: Int): Physical = EntityScanPhysicalOperatorNode(this.groupId + p, this.context, this.entity, this.fetch.map { it.first.copy() to it.second }, p, partitions)

    /**
     * Converts this [EntityScanPhysicalOperatorNode] to a [EntityScanOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     * @return [EntityScanOperator]
     */
    override fun toOperator(ctx: QueryContext): EntityScanOperator = EntityScanOperator(this.groupId, this.entity, this.fetch, this.partitionIndex, this.partitions, ctx)

    /** Generates and returns a [String] representation of this [EntityScanPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.columns.joinToString(",") { it.name.toString() }}]"

    /**
     * Generates and returns a [Digest] for this [EntityScanPhysicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest {
        var result = this.entity.hashCode() + 1L
        result += 31L * result + this.fetch.hashCode()
        result += 31L * result + this.partitionIndex.hashCode()
        result += 31L * result + this.partitions.hashCode()
        return result
    }
}