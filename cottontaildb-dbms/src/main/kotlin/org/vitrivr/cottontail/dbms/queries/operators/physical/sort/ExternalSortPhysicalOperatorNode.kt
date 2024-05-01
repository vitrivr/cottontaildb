package org.vitrivr.cottontail.dbms.queries.operators.physical.sort

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.binding.MissingTuple
import org.vitrivr.cottontail.core.queries.nodes.traits.*
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.sort.ExternalMergeSortOperator
import org.vitrivr.cottontail.dbms.execution.operators.sort.HeapSortOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.statistics.estimateTupleSize

/**
 * A [UnaryPhysicalOperatorNode] that represents sorting the input by a set of specified [ColumnDef]s externally (i.e. with intermediate persistence).
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class ExternalSortPhysicalOperatorNode(input: Physical, val sortOn: List<Pair<Binding.Column, SortOrder>>, val chunkSize: Int) : UnaryPhysicalOperatorNode(input) {
    companion object {
        private const val NODE_NAME = "OrderExt"
    }

    /** The name of this [ExternalSortPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [ExternalSortPhysicalOperatorNode] requires all [ColumnDef]s used on the ORDER BY clause. */
    override val requires: List<Binding.Column> by lazy {
        this.sortOn.map { it.first }
    }

    /** The [Cost] incurred by this [ExternalSortPhysicalOperatorNode]. */
    context(BindingContext, Tuple)
    override val cost: Cost
        get() {
            val tupleSize = this.statistics.estimateTupleSize()
            return Cost(
                io = tupleSize * Cost.DISK_ACCESS_WRITE.io * this.outputSize,
                cpu = 2 * this.sortOn.size * Cost.MEMORY_ACCESS.cpu * this.outputSize,
                memory = (tupleSize * this.chunkSize).toFloat()
            )
        }


    /** The [ExternalSortPhysicalOperatorNode] overwrites/sets the [OrderTrait].  */
    override val traits: Map<TraitType<*>, Trait> by lazy {
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
     * Creates and returns a copy of this [ExternalSortPhysicalOperatorNode] using the given parents as input.
     *
     * @param input The [OperatorNode.Physical]s that act as input.
     * @return Copy of this [ExternalSortPhysicalOperatorNode].
     */
    override fun copyWithNewInput(vararg input: Physical): InMemorySortPhysicalOperatorNode {
        require(input.size == 1) { "The input arity for SortPhysicalOperatorNode.copyWithNewInput() must be 1 but is ${input.size}. This is a programmer's error!"}
        return InMemorySortPhysicalOperatorNode(input = input[0], sortOn = this.sortOn)
    }

    /**
     * Converts this [ExternalSortPhysicalOperatorNode] to a [HeapSortOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator {
        with(ctx.bindings) {
            with(MissingTuple) {
                return ExternalMergeSortOperator(
                    this@ExternalSortPhysicalOperatorNode.input.toOperator(ctx),
                    this@ExternalSortPhysicalOperatorNode.sortOn,
                    this@ExternalSortPhysicalOperatorNode.chunkSize,
                    ctx
                )
            }
        }
    }

    /** Generates and returns a [String] representation of this [ExternalSortPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.sortOn.joinToString(",") { "${it.first.column.name} ${it.second}" }}]"

    /**
     * Generates and returns a [Digest] for this [ExternalSortPhysicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest = this.sortOn.hashCode() + 7L
}