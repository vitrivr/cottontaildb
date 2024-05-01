package org.vitrivr.cottontail.dbms.queries.operators.physical.projection

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.projection.CountProjectionOperator
import org.vitrivr.cottontail.dbms.execution.operators.projection.merge.MergeMaxProjectionOperator
import org.vitrivr.cottontail.dbms.execution.operators.projection.merge.MergeMeanProjectionOperator
import org.vitrivr.cottontail.dbms.execution.operators.projection.merge.MergeMinProjectionOperator
import org.vitrivr.cottontail.dbms.execution.operators.projection.merge.MergeSumProjectionOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.NAryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.projection.Projection


/**
 * A [NAryPhysicalOperatorNode] that represents a projection operation involving aggregate functions such as [Projection.MAX], [Projection.MIN], [Projection.SUM] or [Projection.MEAN].
 *
 * @author Ralph Gasser
 * @version 2.9.0
 */
class MergeAggregatingProjectionPhysicalOperatorNode(vararg inputs: Physical, val type: Projection, override val columns: List<Binding.Column>) : NAryPhysicalOperatorNode(*inputs) {
    /** The name of this [MergeAggregatingProjectionPhysicalOperatorNode]. */
    override val name: String
        get() = this.type.label()

    /** The [ColumnDef] required by this [MergeAggregatingProjectionPhysicalOperatorNode]. */
    override val requires: List<Binding.Column> = this.columns

    /** The output size of this [MergeAggregatingProjectionPhysicalOperatorNode] is always one. */
    context(BindingContext, Tuple)
    override val outputSize: Long
        get() = 1L

    /** The [Cost] of a [MergeAggregatingProjectionPhysicalOperatorNode]. */
    context(BindingContext, Tuple)
    override val cost: Cost
        get() = (Cost.MEMORY_ACCESS + Cost.FLOP) * 3.0f * this.inputs.sumOf { it.outputSize }

    /** The [MergeAggregatingProjectionPhysicalOperatorNode] depends on all its incoming [GroupId]s. */
    override val dependsOn: Array<GroupId> by lazy {
        inputs.map { it.groupId }.toTypedArray()
    }

    /** The [MergeAggregatingProjectionPhysicalOperatorNode] erases all incoming traits. */
    override val traits: Map<TraitType<*>, Trait> = emptyMap()

    init {
        /* Sanity check. */
        require(this.type in arrayOf(Projection.MIN, Projection.MAX, Projection.MAX, Projection.SUM, Projection.MEAN)) {
            "Projection of type ${this.type} cannot be used with instances of AggregatingProjectionPhysicalOperatorNode."
        }
    }

    /**
     * Creates and returns a copy of this [MergeAggregatingProjectionPhysicalOperatorNode] using the given parents as input.
     *
     * @param input The [OperatorNode.Physical]s that act as input.
     * @return Copy of this [MergeAggregatingProjectionPhysicalOperatorNode].
     */
    override fun copyWithNewInput(vararg input: Physical): MergeAggregatingProjectionPhysicalOperatorNode {
        return MergeAggregatingProjectionPhysicalOperatorNode(inputs = input, type = this.type, columns = this.columns)
    }

    /**
     * Converts this [CountProjectionPhysicalOperatorNode] to a [CountProjectionOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator {
        return when (this.type) {
            Projection.SUM -> MergeSumProjectionOperator(this.inputs.map { it.toOperator(ctx.split()) }, this.columns, ctx)
            Projection.MAX -> MergeMaxProjectionOperator(this.inputs.map { it.toOperator(ctx.split()) }, this.columns, ctx)
            Projection.MIN -> MergeMinProjectionOperator(this.inputs.map { it.toOperator(ctx.split()) }, this.columns, ctx)
            Projection.MEAN -> MergeMeanProjectionOperator(this.inputs.map { it.toOperator(ctx.split()) }, this.columns, ctx)
            else -> throw IllegalStateException("An AggregatingProjectionPhysicalOperatorNode requires a project of type SUM, MAX, MIN or MEAN but encountered ${this.type}.")
        }
    }

    /** Generates and returns a [String] representation of this [MergeAggregatingProjectionPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.columns.joinToString(",") { it.column.name.toString() }}]"

    /**
     * Compares this [MergeAggregatingProjectionPhysicalOperatorNode] to another object.
     *
     * @param other The other [Any] to compare this [MergeAggregatingProjectionPhysicalOperatorNode] to.
     * @return True if other equals this, false otherwise.
     */
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is MergeAggregatingProjectionPhysicalOperatorNode) return false
        if (this.type != other.type) return false
        if (this.columns != other.columns) return false
        return true
    }

    /**
     * Generates and returns a hash code for this [MergeAggregatingProjectionPhysicalOperatorNode].
     */
    override fun hashCode(): Int {
        var result = this.type.hashCode()
        result += 32 * result + this.columns.hashCode()
        return result
    }

    /**
     * Generates and returns a [Digest] for this [MergeAggregatingProjectionPhysicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest {
        var result = this.type.hashCode() + 5L
        result += 33L * result + this.columns.hashCode()
        return result
    }
}