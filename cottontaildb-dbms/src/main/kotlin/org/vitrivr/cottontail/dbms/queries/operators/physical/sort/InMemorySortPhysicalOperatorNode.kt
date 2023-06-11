package org.vitrivr.cottontail.dbms.queries.operators.physical.sort

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.binding.MissingTuple
import org.vitrivr.cottontail.core.queries.nodes.traits.*
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.sort.HeapSortOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.statistics.estimateTupleSize

/**
 * A [UnaryPhysicalOperatorNode] that represents sorting the input by a set of specified [ColumnDef]s in-memory.
 *
 * @author Ralph Gasser
 * @version 2.4.0
 */
class InMemorySortPhysicalOperatorNode(input: Physical, val sortOn: List<Pair<ColumnDef<*>, SortOrder>>) : UnaryPhysicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Order"
    }

    /** The name of this [InMemorySortPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [InMemorySortPhysicalOperatorNode] requires all [ColumnDef]s used on the ORDER BY clause. */
    override val requires: List<ColumnDef<*>> = this.sortOn.map { it.first }

    /** The [Cost] incurred by this [InMemorySortPhysicalOperatorNode]. */
    context(BindingContext, Tuple)
    override val cost: Cost
        get() = Cost(
            cpu = 2 * this.sortOn.size * Cost.MEMORY_ACCESS.cpu,
            memory = this.statistics.estimateTupleSize().toFloat()
        ) * this.outputSize


    /** The [InMemorySortPhysicalOperatorNode] overwrites/sets the [OrderTrait].  */
    override val traits: Map<TraitType<*>,Trait> by lazy {
        super.traits + listOf(
            OrderTrait to OrderTrait(this.sortOn),
            MaterializedTrait to MaterializedTrait,
            NotPartitionableTrait to NotPartitionableTrait
        )
    }

    init {
        if (this.sortOn.isEmpty())
            throw QueryException.QuerySyntaxException("At least one column must be specified for sorting.")
    }

    /**
     * Creates and returns a copy of this [InMemorySortPhysicalOperatorNode] using the given parents as input.
     *
     * @param input The [OperatorNode.Physical]s that act as input.
     * @return Copy of this [InMemorySortPhysicalOperatorNode].
     */
    override fun copyWithNewInput(vararg input: Physical): InMemorySortPhysicalOperatorNode {
        require(input.size == 1) { "The input arity for SortPhysicalOperatorNode.copyWithNewInput() must be 1 but is ${input.size}. This is a programmer's error!"}
        return InMemorySortPhysicalOperatorNode(input = input[0], sortOn = this.sortOn)
    }

    /**
     * Converts this [InMemorySortPhysicalOperatorNode] to a [HeapSortOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator {
        with(ctx.bindings) {
            with(MissingTuple) {
                require(this@InMemorySortPhysicalOperatorNode.input.outputSize < Integer.MAX_VALUE.toLong()) { "In-memory sorting cannot be applied to data sets with more than ${Integer.MAX_VALUE} entries." }
                return HeapSortOperator(
                    this@InMemorySortPhysicalOperatorNode.input.toOperator(ctx),
                    this@InMemorySortPhysicalOperatorNode.sortOn,
                    this@InMemorySortPhysicalOperatorNode.outputSize.coerceAtMost(Integer.MAX_VALUE.toLong()).toInt(),
                    ctx
                )
            }
        }
    }

    /** Generates and returns a [String] representation of this [InMemorySortPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.sortOn.joinToString(",") { "${it.first.name} ${it.second}" }}]"

    /**
     * Generates and returns a [Digest] for this [InMemorySortPhysicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest = this.sortOn.hashCode() + 3L
}