package org.vitrivr.cottontail.dbms.queries.operators.physical.sort

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.nodes.traits.LimitTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.OrderTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.sort.LimitingHeapSortOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.merge.MergeLimitingSortPhysicalOperatorNode
import kotlin.math.min

/**
 * A [UnaryPhysicalOperatorNode] that represents sorting the input by a set of specified [ColumnDef]s but limiting the output to the
 * top K entries. This is semantically equivalent to a ORDER BY XY LIMIT Z. Internally, a heap sort algorithm is employed for sorting.
 *
 * @author Ralph Gasser
 * @version 2.2.0
 */
class LimitingSortPhysicalOperatorNode(input: Physical? = null, val sortOn: List<Pair<ColumnDef<*>, SortOrder>>, val limit: Long, val skip: Long) : UnaryPhysicalOperatorNode(input) {
    companion object {
        private const val NODE_NAME = "OrderAndLimit"
    }

    /** The name of this [SortPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [LimitingSortPhysicalOperatorNode] requires all [ColumnDef]s used on the ORDER BY clause. */
    override val requires: List<ColumnDef<*>> = sortOn.map { it.first }

    /** The size of the output produced by this [SortPhysicalOperatorNode]. */
    override val outputSize: Long = min((super.outputSize - this.skip), this.limit)

    /** The [LimitingSortPhysicalOperatorNode] does not allow for partitioning. */
    override val canBePartitioned: Boolean
        get() = false

    /** The [Cost] incurred by this [SortPhysicalOperatorNode]. */
    override val cost: Cost
        get() = Cost(
            cpu = 2 * (this.input?.outputSize ?: 0) * this.sortOn.size * Cost.MEMORY_ACCESS.cpu,
            memory = (this.columns.sumOf {
                if (it.type == Types.String) {
                    this.statistics[it]!!.avgWidth * Char.SIZE_BYTES
                } else {
                    it.type.physicalSize
                }
            } * this.outputSize).toFloat()
        )

    /** The [LimitingSortPhysicalOperatorNode] overwrites/sets the [OrderTrait] and the [LimitTrait].  */
    override val traits: Map<TraitType<*>, Trait>
        get() = super.traits + listOf(OrderTrait to OrderTrait(this.sortOn), LimitTrait to LimitTrait(this.skip, this.limit))

    init {
        if (this.sortOn.isEmpty()) throw QueryException.QuerySyntaxException("At least one column must be specified for sorting.")
    }

    /**
     * Creates and returns a copy of this [LimitingSortPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [LimitingSortPhysicalOperatorNode].
     */
    override fun copy() = LimitingSortPhysicalOperatorNode(sortOn = this.sortOn, limit = this.limit, skip = this.skip)

    /**
     * Creates and returns a copy of this [LimitingSortPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [LimitingSortPhysicalOperatorNode].
     */
    override fun tryPartition(partitions: Int): Physical? {
        require(partitions > 1) { "Expected number of partitions must be greater than one but encountered $partitions." }
        val input = this.input ?: throw IllegalStateException("Tried to propagate call to tryPartition($partitions) to an absent input. This is a programmer's error!")
        return if (input.canBePartitioned) {
            val inbound = (0 until partitions).map { input.partition(partitions, it) }
            this.copyWithOutput(MergeLimitingSortPhysicalOperatorNode(*inbound.toTypedArray(), sortOn = this.sortOn, limit = this.limit))
        } else {
            this.input?.tryPartition(partitions)
        }
    }

    /**
     * Converts this [LimitingSortPhysicalOperatorNode] to a [LimitingHeapSortOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator {
        val input = this.input ?: throw IllegalStateException("Cannot convert disconnected OperatorNode to Operator (node = $this)")
        return LimitingHeapSortOperator(input.toOperator(ctx), this.sortOn, this.limit, this.skip)
    }

    /** Generates and returns a [String] representation of this [SortPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.sortOn.joinToString(",") { "${it.first.name} ${it.second}" }},${this.skip},${this.limit}]"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is LimitingSortPhysicalOperatorNode) return false

        if (skip != other.skip) return false
        if (limit != other.limit) return false
        if (this.sortOn != other.sortOn) return false

        return true
    }

    override fun hashCode(): Int {
        var result = skip.hashCode()
        result = 31 * result + limit.hashCode()
        result = 31 * result + sortOn.hashCode()
        return result
    }
}