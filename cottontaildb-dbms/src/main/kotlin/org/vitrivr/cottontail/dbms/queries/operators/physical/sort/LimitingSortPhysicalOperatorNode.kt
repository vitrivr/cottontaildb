package org.vitrivr.cottontail.dbms.queries.operators.physical.sort

import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.binding.MissingTuple
import org.vitrivr.cottontail.core.queries.nodes.traits.*
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.execution.operators.sort.LimitingHeapSortOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.merge.MergeLimitingSortPhysicalOperatorNode
import kotlin.math.min

/**
 * A [UnaryPhysicalOperatorNode] that represents sorting the input by a set of specified [ColumnDef]s but limiting the output to the
 * top K entries. This is semantically equivalent to a ORDER BY XY LIMIT Z. Internally, a heap sort algorithm is employed for sorting.
 *
 * @author Ralph Gasser
 * @version 2.4.0
 */
class LimitingSortPhysicalOperatorNode(input: Physical, val sortOn: List<Pair<ColumnDef<*>, SortOrder>>, val limit: Long) : UnaryPhysicalOperatorNode(input) {
    companion object {
        private const val NODE_NAME = "OrderAndLimit"
    }

    /** The name of this [SortPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [LimitingSortPhysicalOperatorNode] requires all [ColumnDef]s used on the ORDER BY clause. */
    override val requires: List<ColumnDef<*>> = sortOn.map { it.first }

    /** The size of the output produced by this [SortPhysicalOperatorNode]. */
    context(BindingContext, Tuple)
    override val outputSize: Long
        get() = min(super.outputSize, this.limit)

    /** The [Cost] incurred by this [SortPhysicalOperatorNode]. */
    context(BindingContext, Tuple)
    override val cost: Cost
        get() = Cost(
            cpu = 2 * this.sortOn.size * Cost.MEMORY_ACCESS.cpu,
            memory = (this.columns.sumOf {
                if (it.type == Types.String) {
                    this.statistics[it]!!.avgWidth * Char.SIZE_BYTES
                } else {
                    it.type.physicalSize
                }
            }).toFloat()
        ) * this.outputSize

    /** The [LimitingSortPhysicalOperatorNode] overwrites/sets the [OrderTrait] and the [LimitTrait].  */
    override val traits: Map<TraitType<*>, Trait>
        get() = super.traits + mapOf(
            OrderTrait to OrderTrait(this.sortOn),
            LimitTrait to LimitTrait(this.limit),
            MaterializedTrait to MaterializedTrait,
            NotPartitionableTrait to NotPartitionableTrait /* Once explicit sorting has been introduced, no more partitioning is possible. */
        )

    init {
        if (this.sortOn.isEmpty()) throw QueryException.QuerySyntaxException("At least one column must be specified for sorting.")
    }

    /**
     * Creates and returns a copy of this [LimitingSortPhysicalOperatorNode] using the given parents as input.
     *
     * @param input The [OperatorNode.Physical]s that act as input.
     * @return Copy of this [LimitingSortPhysicalOperatorNode].
     */
    override fun copyWithNewInput(vararg input: Physical): LimitingSortPhysicalOperatorNode {
        require(input.size == 1) { "The input arity for SkipPhysicalOperatorNode.copyWithNewInput() must be 1 but is ${input.size}. This is a programmer's error!"}
        return LimitingSortPhysicalOperatorNode(input = input[0], sortOn = this.sortOn, limit = this.limit)
    }

    /**
     * For this operator, there is a dedicated, [MergeLimitingSortPhysicalOperatorNode] that acts as a drop-in replacement,
     * if the input allows for partitioning.
     *
     * @param ctx The [QueryContext] to use when determining the optimal number of partitions.
     * @param max The maximum number of partitions to create.
     * @return [OperatorNode.Physical] operator at the based of the new query plan.
     */
    override fun tryPartition(ctx: QueryContext, max: Int): Physical? {
        return if (!this.input.hasTrait(NotPartitionableTrait)) {
            val partitions = with(ctx.bindings) {
                with(MissingTuple) {
                    ctx.costPolicy.parallelisation(this@LimitingSortPhysicalOperatorNode.parallelizableCost, this@LimitingSortPhysicalOperatorNode.totalCost, max)
                }
            }
            if (partitions <= 1) return null
            val inbound = (0 until partitions).map { input.partition(partitions, it) }
            val outbound = this.output
            val merge = MergeLimitingSortPhysicalOperatorNode(*inbound.toTypedArray(), sortOn = this.sortOn, limit = this.limit)
            outbound?.copyWithOutput(merge) ?: merge
        } else {
            this.input.tryPartition(ctx, max)
        }
    }

    /**
     * Converts this [LimitingSortPhysicalOperatorNode] to a [LimitingHeapSortOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext) = LimitingHeapSortOperator(this.input.toOperator(ctx), this.sortOn, this.limit, ctx)

    /** Generates and returns a [String] representation of this [SortPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.sortOn.joinToString(",") { "${it.first.name} ${it.second}" }},${this.limit}]"

    /**
     * Generates and returns a [Digest] for this [LimitingSortPhysicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest {
        var result = this.limit.hashCode().toLong() + 1L
        result += 31L * result + this.sortOn.hashCode()
        return result
    }
}