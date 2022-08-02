package org.vitrivr.cottontail.dbms.queries.operators.physical.transform

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.transform.FetchOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.statistics.columns.ValueStatistics

/**
 * A [UnaryPhysicalOperatorNode] that represents fetching certain [ColumnDef] from a specific [Entity] and
 * adding them to the list of retrieved [ColumnDef]s.
 *
 * This can be used for late population, which can lead to optimized performance for kNN queries
 *
 * @author Ralph Gasser
 * @version 2.4.0
 */
@Suppress("UNCHECKED_CAST")
class FetchPhysicalOperatorNode(input: Physical, val entity: EntityTx, val fetch: List<Pair<Binding.Column, ColumnDef<*>>>) : UnaryPhysicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Fetch"
    }

    /** The name of this [FetchPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [FetchPhysicalOperatorNode] accesses the [ColumnDef] of its input + the columns to be fetched. */
    override val physicalColumns: List<ColumnDef<*>>
        get() = super.physicalColumns + this.fetch.map { it.second }

    /** The [FetchPhysicalOperatorNode] returns the [ColumnDef] of its input + the columns to be fetched. */
    override val columns: List<ColumnDef<*>>
        get() = super.columns + this.fetch.map { it.first.column }

    /** The map of [ValueStatistics] employed by this [FetchPhysicalOperatorNode]. */
    override val statistics: Map<ColumnDef<*>, ValueStatistics<*>>
        get() = super.statistics + this.localStatistics

    /** The [Cost] of a [FetchPhysicalOperatorNode]. */
    context(BindingContext,Record)
    override val cost: Cost
        get() = (Cost.DISK_ACCESS_READ + Cost.MEMORY_ACCESS) * this.outputSize * this.fetch.sumOf { (b, _) ->
            if (b.type == Types.String) {
                this.localStatistics[b.column]!!.avgWidth * Char.SIZE_BYTES
            } else {
                b.type.physicalSize
            }
        } * 10.0f /* Random access cost is more expensive. */


    /** Local reference to entity statistics. */
    private val localStatistics = Object2ObjectLinkedOpenHashMap<ColumnDef<*>, ValueStatistics<*>>()

    /* Initialize local statistics. */
    init {
        var fetchSize = 0
        for ((binding, physical) in this.fetch) {
            if (!this.localStatistics.containsKey(binding.column)) {
                this.localStatistics[binding.column] = this.entity.columnForName(physical.name).newTx(this.entity.context).statistics() as ValueStatistics<Value>
            }
        }
    }

    /**
     * Creates and returns a copy of this [FetchPhysicalOperatorNode] using the given parents as input.
     *
     * @param input The [OperatorNode.Physical]s that act as input.
     * @return Copy of this [FetchPhysicalOperatorNode].
     */
    override fun copyWithNewInput(vararg input: Physical): FetchPhysicalOperatorNode {
        require(input.size == 1) { "The input arity for FetchPhysicalOperatorNode.copyWithNewInput() must be 1 but is ${input.size}. This is a programmer's error!"}
        return FetchPhysicalOperatorNode(input = input[0], entity = this.entity, fetch = this.fetch.map { it.first.copy() to it.second })
    }

    /**
     * Converts this [FetchPhysicalOperatorNode] to a [FetchOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): FetchOperator {
        /* Generate and return FetchOperator. */
        return FetchOperator(this.input.toOperator(ctx), this.entity, this.fetch, ctx)
    }

    /** Generates and returns a [String] representation of this [FetchPhysicalOperatorNode]. */
    override fun toString() = "${this.groupId}:Fetch[${this.fetch.joinToString(",") { it.second.name.toString() }}]"

    /**
     * Generates and returns a [Digest] for this [FetchPhysicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest {
        var result = this.entity.dbo.name.hashCode().toLong()
        result += 31L * result + this.fetch.hashCode()
        return result
    }
}