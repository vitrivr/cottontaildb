package org.vitrivr.cottontail.dbms.queries.operators.physical.sort

import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.nodes.traits.MaterializedTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.OrderTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.core.recordset.PlaceholderRecord
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.sort.HeapSortOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.UnaryPhysicalOperatorNode

/**
 * A [UnaryPhysicalOperatorNode] that represents sorting the input by a set of specified [ColumnDef]s. Internally,
 * a heap sort algorithm is applied for sorting.
 *
 * @author Ralph Gasser
 * @version 2.3.0
 */
class SortPhysicalOperatorNode(input: Physical, val sortOn: List<Pair<ColumnDef<*>, SortOrder>>) : UnaryPhysicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Order"
    }

    /** The name of this [SortPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [SortPhysicalOperatorNode] requires all [ColumnDef]s used on the ORDER BY clause. */
    override val requires: List<ColumnDef<*>> = sortOn.map { it.first }

    /** The [Cost] incurred by this [SortPhysicalOperatorNode]. */
    context(BindingContext,Record)    override val cost: Cost
        get() = Cost(
            cpu = 2 * this.sortOn.size * Cost.MEMORY_ACCESS.cpu,
            memory = this.columns.sumOf {
                if (it.type == Types.String) {
                    (this.statistics[it]?.avgWidth ?: 1) * Char.SIZE_BYTES
                } else {
                    it.type.physicalSize
                }
            }.toFloat()
        ) * this.outputSize


    /** The [SortPhysicalOperatorNode] overwrites/sets the [OrderTrait].  */
    override val traits: Map<TraitType<*>,Trait>
        get() = super.traits + listOf(
            OrderTrait to OrderTrait(this.sortOn),
            MaterializedTrait to MaterializedTrait
        )

    init {
        if (this.sortOn.isEmpty()) throw QueryException.QuerySyntaxException("At least one column must be specified for sorting.")
    }

    /**
     * Creates and returns a copy of this [SortPhysicalOperatorNode] using the given parents as input.
     *
     * @param input The [OperatorNode.Physical]s that act as input.
     * @return Copy of this [SortPhysicalOperatorNode].
     */
    override fun copyWithNewInput(vararg input: Physical): SortPhysicalOperatorNode {
        require(input.size == 1) { "The input arity for SortPhysicalOperatorNode.copyWithNewInput() must be 1 but is ${input.size}. This is a programmer's error!"}
        return SortPhysicalOperatorNode(input = input[0], sortOn = this.sortOn)
    }

    /**
     * Converts this [SortPhysicalOperatorNode] to a [HeapSortOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator {
        with(ctx.bindings) {
            with(PlaceholderRecord) {
                return HeapSortOperator(
                    this@SortPhysicalOperatorNode.input.toOperator(ctx),
                    this@SortPhysicalOperatorNode.sortOn,
                    this@SortPhysicalOperatorNode.outputSize.coerceAtMost(Integer.MAX_VALUE.toLong()).toInt(),
                    ctx
                )
            }
        }
    }

    /** Generates and returns a [String] representation of this [SortPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.sortOn.joinToString(",") { "${it.first.name} ${it.second}" }}]"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is SortPhysicalOperatorNode) return false
        if (this.sortOn != other.sortOn) return false
        return true
    }

    override fun hashCode(): Int = this.sortOn.hashCode()
}