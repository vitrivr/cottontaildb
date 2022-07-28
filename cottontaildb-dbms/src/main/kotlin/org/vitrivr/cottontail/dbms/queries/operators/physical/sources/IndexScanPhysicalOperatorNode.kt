package org.vitrivr.cottontail.dbms.queries.operators.physical.sources

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.MissingRecord
import org.vitrivr.cottontail.core.queries.nodes.traits.*
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.BooleanPredicate
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.sources.IndexScanOperator
import org.vitrivr.cottontail.dbms.index.basic.AbstractIndex
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.index.basic.IndexTx
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.merge.MergeLimitingSortPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.merge.MergePhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.transform.LimitPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.statistics.columns.ValueStatistics
import org.vitrivr.cottontail.dbms.statistics.selectivity.NaiveSelectivityCalculator
import org.vitrivr.cottontail.dbms.statistics.selectivity.Selectivity

/**
 * A [IndexScanPhysicalOperatorNode] that represents a predicated lookup using an [AbstractIndex].
 *
 * @author Ralph Gasser
 * @version 2.6.0
 */
@Suppress("UNCHECKED_CAST")
class IndexScanPhysicalOperatorNode(override val groupId: Int,
                                    val index: IndexTx,
                                    val predicate: Predicate,
                                    val fetch: List<Pair<Binding.Column, ColumnDef<*>>>,
                                    val partitionIndex: Int = 0,
                                    val partitions: Int = 1) : NullaryPhysicalOperatorNode() {
    companion object {
        private const val NODE_NAME = "ScanIndex"
    }

    /** The name of this [IndexScanPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [ColumnDef]s accessed by this [IndexScanPhysicalOperatorNode] depends on the [ColumnDef]s produced by the [Index]. */
    override val physicalColumns: List<ColumnDef<*>>

    /** The [ColumnDef]s produced by this [IndexScanPhysicalOperatorNode] depends on the [ColumnDef]s produced by the [Index]. */
    override val columns: List<ColumnDef<*>>

    /** [IndexScanPhysicalOperatorNode] are always executable. */
    override val executable: Boolean = true

    /** [ValueStatistics] are taken from the underlying [Entity]. The query planner uses statistics for [Cost] estimation. */
    override val statistics = Object2ObjectLinkedOpenHashMap<ColumnDef<*>, ValueStatistics<*>>()

    /** Cost estimation for [IndexScanPhysicalOperatorNode]s is delegated to the [Index]. */
    override val cost: Cost by lazy {
        this.index.costFor(this.predicate)
    }

    /** Returns [Map] of [Trait]s for this [IndexScanOperator], which is derived directly from the [Index]*/
    override val traits: Map<TraitType<*>, Trait> by lazy {
        val indexTraits = this.index.traitsFor(this.predicate)
        val traits = mutableMapOf<TraitType<*>,Trait>()
        for ((type, trait) in indexTraits) {
            when (type) {
                OrderTrait -> { /* Map physical columns to bound columns. */
                    val order = (trait as OrderTrait)
                    val newOrder = order.order.map { (c1, o) ->
                        val find = this.fetch.single { (_, c2) -> c2 == c1 }
                        find.first.column to o
                    }
                    traits[type] = OrderTrait(newOrder)
                }
                else -> traits[type] = trait
            }
        }
        traits
    }

    /** The estimated output size of this [IndexScanPhysicalOperatorNode]. */
    override val outputSize: Long by lazy {
        with(MissingRecord) {
            with(this@IndexScanPhysicalOperatorNode.index.context.bindings) {
                when (val predicate = this@IndexScanPhysicalOperatorNode.predicate) {
                    is ProximityPredicate.Scan -> this@IndexScanPhysicalOperatorNode.index.count()
                    is ProximityPredicate.NNS -> predicate.k
                    is ProximityPredicate.FNS -> predicate.k
                    is ProximityPredicate.ENN -> Selectivity.DEFAULT(this@IndexScanPhysicalOperatorNode.index.count())
                    is BooleanPredicate -> {
                        val entityTx = this@IndexScanPhysicalOperatorNode.index.dbo.parent.newTx(this@IndexScanPhysicalOperatorNode.index.context)
                        NaiveSelectivityCalculator.estimate(predicate, this@IndexScanPhysicalOperatorNode.statistics)(entityTx.count())
                    }
                }
            }
        }
    }

    init {
        val entityTx = this.index.dbo.parent.newTx(this.index.context)
        val columns = mutableListOf<ColumnDef<*>>()
        val physicalColumns = mutableListOf<ColumnDef<*>>()
        val indexProduces = this.index.columnsFor(this.predicate)
        val entityProduces = entityTx.listColumns()

        /* Produce statistics. */
        for ((binding, physical) in this.fetch) {
            require(indexProduces.contains(physical)) { "The given column $physical is not produced by the selected index ${this.index.dbo}. This is a programmer's error!"}

            /* Populate list of columns. */
            columns.add(binding.column)
            physicalColumns.add(physical)

            /* Populate statistics. */
            if (!this.statistics.containsKey(binding.column) && entityProduces.contains(physical)) {
                this.statistics[binding.column] = entityTx.columnForName(physical.name).newTx(this.index.context).statistics() as ValueStatistics<Value>
            }
        }

        /* Initialize local fields. */
        this.columns = columns
        this.physicalColumns = physicalColumns
    }

    /**
     * Creates and returns a copy of this [IndexScanPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [IndexScanPhysicalOperatorNode].
     */
    override fun copy() = IndexScanPhysicalOperatorNode(this.groupId, this.index, this.predicate, this.fetch.map { it.first.copy() to it.second })

    /**
     * [IndexScanPhysicalOperatorNode] can be partitioned if the underlying input allows for partitioning.
     *
     * @param ctx The [QueryContext] to use when determining the optimal number of partitions.
     * @param max The maximum number of partitions to create.
     * @return [OperatorNode.Physical] operator at the based of the new query plan.
     */
    override fun tryPartition(ctx: QueryContext, max: Int): Physical? {
        if (this.hasTrait(NotPartitionableTrait)) return null

        /* Determine optimal number of partitions and create them. */
        val partitions = with(ctx.bindings) {
            with(MissingRecord) {
                ctx.costPolicy.parallelisation(this@IndexScanPhysicalOperatorNode.parallelizableCost, this@IndexScanPhysicalOperatorNode.totalCost, max)
            }
        }
        if (partitions <= 1) return null
        val inbound = (0 until partitions).map { this.partition(partitions, it) }

        /* Merge strategy depends on the traits of the underlying index. */
        val merge = when {
            this.hasTrait(LimitTrait) && this.hasTrait(OrderTrait) -> {
                val order = this[OrderTrait]!!
                val limit = this[LimitTrait]!!
                MergeLimitingSortPhysicalOperatorNode(*inbound.toTypedArray(), sortOn = order.order, limit = limit.limit)
            }
            this.hasTrait(LimitTrait) -> {
                val limit = this[LimitTrait]!!
                LimitPhysicalOperatorNode(MergePhysicalOperatorNode(*inbound.toTypedArray()), limit = limit.limit)
            }
            this.hasTrait(OrderTrait) -> {
                TODO()
            }
            else -> MergePhysicalOperatorNode(*inbound.toTypedArray())
        }
        return this.output?.copyWithOutput(merge)
    }

    /**
     * Generates a partitioned version of this [IndexScanPhysicalOperatorNode].
     *
     * @param partitions The total number of partitions.
     * @param p The partition index.
     * @return Partitioned [IndexScanPhysicalOperatorNode]
     */
    override fun partition(partitions: Int, p: Int): Physical {
        check(!this.hasTrait(NotPartitionableTrait)) { "IndesScanPhysicalOperatorNode with index ${this.index.dbo.name} cannot be partitioned. This is a programmer's error!"}
        return IndexScanPhysicalOperatorNode(this.groupId + p, this.index, this.predicate, this.fetch.map { it.first.copy() to it.second }, p, partitions)
    }

    /**
     * Converts this [IndexScanPhysicalOperatorNode] to a [IndexScanOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator {
        /** Generate and return IndexScanOperator. */
        return IndexScanOperator(this.groupId, this.index, this.predicate, this.fetch, this.partitionIndex, this.partitions, ctx)
    }

    /** Generates and returns a [String] representation of this [EntityScanPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.index.dbo.type},${this.predicate}]"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is IndexScanPhysicalOperatorNode) return false

        if (depth != other.depth) return false
        if (predicate != other.predicate) return false

        return true
    }

    override fun hashCode(): Int {
        var result = depth.hashCode()
        result = 31 * result + predicate.hashCode()
        return result
    }
}