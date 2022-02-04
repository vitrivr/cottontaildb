package org.vitrivr.cottontail.dbms.queries.operators.physical.sort

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.sort.LimitingHeapSortOperator
import org.vitrivr.cottontail.dbms.queries.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.merge.MergeLimitingSortPhysicalOperator
import org.vitrivr.cottontail.dbms.queries.sort.SortOrder
import kotlin.math.min

/**
 * A [UnaryPhysicalOperatorNode] that represents sorting the input by a set of specified [ColumnDef]s but limiting the output to the
 * top K entries. This is semantically equivalent to a ORDER BY XY LIMIT Z. Internally, a heap sort algorithm is employed for sorting.
 *
 * @author Ralph Gasser
 * @version 2.2.0
 */
class LimitingSortPhysicalOperatorNode(input: Physical? = null, override val sortOn: List<Pair<ColumnDef<*>, SortOrder>>, val limit: Long, val skip: Long) : UnaryPhysicalOperatorNode(input) {
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
            cpu = 2 * (this.input?.outputSize ?: 0) * this.sortOn.size * Cost.COST_MEMORY_ACCESS,
            memory = (this.columns.sumOf {
                if (it.type == Types.String) {
                    this.statistics[it].avgWidth * Char.SIZE_BYTES
                } else {
                    it.type.physicalSize
                }
            } * this.outputSize).toFloat()
        )

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
     * Converts this [LimitingSortPhysicalOperatorNode] to a [LimitingHeapSortOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator {
        val input = this.input ?: throw IllegalStateException("Cannot convert disconnected OperatorNode to Operator (node = $this)")
        return LimitingHeapSortOperator(input.toOperator(ctx), this.sortOn, this.limit, this.skip)
    }

    /**
     * Tries to create a partitioned version of this [LimitingHeapSortOperator] and its parents.
     *
     * A [LimitingHeapSortOperator] allows for an optimized version
     *
     * @param p The desired number of partitions. If null, the value will be determined automatically.
     * @return Array of [OperatorNode.Physical]s.
     */
    override fun tryPartition(partitions: Int, p: Int?): Physical? {
        val input = this.input ?: return null
        if (p != null) { /* If p is set, simply copy and propagate upwards. */
            val copy = this.copy()
            copy.input = (this.input?.tryPartition(partitions, p) ?: throw IllegalStateException("Tried to propagate call to tryPartition($partitions, $p), which returned null. This is a programmer's error!"))
            return copy
        } else if (input.canBePartitioned) {
            val inbound = (0 until partitions).map {
                input.tryPartition(partitions, it) ?: throw IllegalStateException("Tried to propagate call to tryPartition($partitions, $it), which returned null. This is a programmer's error!")
            }
            val merge = MergeLimitingSortPhysicalOperator(inputs = inbound.toTypedArray(), sortOn = this.sortOn, limit = this.limit)
            return this.output?.copyWithOutput(merge)
        }
        val newp = this.totalCost.parallelisation()
        return input.tryPartition(newp)
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