package org.vitrivr.cottontail.dbms.queries.operators.physical.projection

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.binding.MissingTuple
import org.vitrivr.cottontail.core.queries.nodes.traits.NotPartitionableTrait
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.projection.*
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.projection.Projection

/**
 * A [UnaryPhysicalOperatorNode] that represents a projection operation involving aggregate functions such as [Projection.MAX], [Projection.MIN], [Projection.SUM] or [Projection.MEAN].
 *
 * @author Ralph Gasser
 * @version 2.9.0
 */
class AggregatingProjectionPhysicalOperatorNode(input: Physical, val type: Projection, override val columns: List<Binding.Column>) : UnaryPhysicalOperatorNode(input) {
    /** The name of this [AggregatingProjectionPhysicalOperatorNode]. */
    override val name: String
        get() = this.type.label()

    init {
        require(this.columns.all { it.type is Types.Numeric }) { "Projection of type ${this.type} can only be applied to numeric column, which $columns isn't." }
    }

    /** The [ColumnDef] required by this [AggregatingProjectionPhysicalOperatorNode]. */
    override val requires: List<Binding.Column> = this.columns

    /** The output size of this [AggregatingProjectionPhysicalOperatorNode] is always one. */
    context(BindingContext, Tuple)
    override val outputSize: Long
        get() = 1L

    /** The [Cost] of a [AggregatingProjectionPhysicalOperatorNode]. */
    context(BindingContext, Tuple)
    override val cost: Cost
       get() = (Cost.MEMORY_ACCESS + Cost.FLOP) * 3.0f * this.input.outputSize

    init {
        /* Sanity check. */
        require(this.type in arrayOf(Projection.MIN, Projection.MAX, Projection.MAX, Projection.SUM, Projection.MEAN)) {
            "Projection of type ${this.type} cannot be used with instances of AggregatingProjectionPhysicalOperatorNode."
        }
    }

    /**
     * Creates and returns a copy of this [AggregatingProjectionPhysicalOperatorNode] using the given parents as input.
     *
     * @param input The [OperatorNode.Physical]s that act as input.
     * @return Copy of this [AggregatingProjectionPhysicalOperatorNode].
     */
    override fun copyWithNewInput(vararg input: Physical): AggregatingProjectionPhysicalOperatorNode {
        require(input.size == 1) { "The input arity for AggregatingProjectionPhysicalOperatorNode.copyWithNewInput() must be 1 but is ${input.size}. This is a programmer's error!"}
        return AggregatingProjectionPhysicalOperatorNode(input = input[0], type = this.type, columns = this.columns)
    }

    /**
     * Converts this [CountProjectionPhysicalOperatorNode] to a [CountProjectionOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator {
        return when (this.type) {
            Projection.SUM -> SumProjectionOperator(this.input.toOperator(ctx), this.columns, ctx)
            Projection.MAX -> MaxProjectionOperator(this.input.toOperator(ctx), this.columns, ctx)
            Projection.MIN -> MinProjectionOperator(this.input.toOperator(ctx), this.columns, ctx)
            Projection.MEAN -> MeanProjectionOperator(this.input.toOperator(ctx), this.columns, ctx)
            else -> throw IllegalStateException("An AggregatingProjectionPhysicalOperatorNode requires a project of type SUM, MAX, MIN or MEAN but encountered ${this.type}.")
        }
    }

    /**
     * Tries to create a partitioned version of this [AggregatingProjectionPhysicalOperatorNode] and its parents.
     *
     * In general, partitioning is possible if the [input] doesn't have a [NotPartitionableTrait]. Otherwise, [UnaryPhysicalOperatorNode]
     * propagates this call up a tree until a [OperatorNode.Physical] that does not have this trait is reached.
     *
     * @return Array of [OperatorNode.Physical]s.
     */
    override fun tryPartition(ctx: QueryContext, max: Int): Physical? {
        require(max > 1) { "Expected number of partitions to be greater than one but encountered $max." }
        return if (!this.input.hasTrait(NotPartitionableTrait)) {
            val partitions = with(ctx.bindings) {
                with(MissingTuple) {
                    ctx.costPolicy.parallelisation(this@AggregatingProjectionPhysicalOperatorNode.parallelizableCost, this@AggregatingProjectionPhysicalOperatorNode.totalCost, max)
                }
            }
            if (partitions <= 1) return null
            val inbound = (0 until partitions).map { this.input.partition(partitions, it) }.toTypedArray()
            return MergeAggregatingProjectionPhysicalOperatorNode(inputs = inbound, type = this.type, columns = this.columns)
        } else {
            this.input.tryPartition(ctx, max)
        }
    }


    /** Generates and returns a [String] representation of this [AggregatingProjectionPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.columns.joinToString(",") { it.column.name.toString() }}]"

    /**
     * Generates and returns a [Digest] for this [AggregatingProjectionPhysicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest {
        var result = this.type.hashCode().toLong()
        result += 31L * result + this.columns.hashCode()
        return result
    }
}