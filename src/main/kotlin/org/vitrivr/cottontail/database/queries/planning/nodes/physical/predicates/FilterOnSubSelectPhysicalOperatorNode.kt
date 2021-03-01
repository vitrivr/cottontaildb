package org.vitrivr.cottontail.database.queries.planning.nodes.physical.predicates

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.BindingContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.BinaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NAryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate
import org.vitrivr.cottontail.database.queries.predicates.bool.ComparisonOperator
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.predicates.FilterOnSubselectOperator
import org.vitrivr.cottontail.model.values.types.Value

/**
 * A [BinaryLogicalOperatorNode] that formalizes filtering using some [BooleanPredicate].
 *
 * As opposed to [FilterPhysicalOperatorNode], the [FilterOnSubSelectPhysicalOperatorNode] depends on
 * the execution of one or many sub-queries.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class FilterOnSubSelectPhysicalOperatorNode(val predicate: BooleanPredicate, vararg inputs: OperatorNode.Physical) : NAryPhysicalOperatorNode(*inputs) {

    /** The [FilterOnSubSelectPhysicalOperatorNode] returns the [ColumnDef] of its input. */
    override val columns: Array<ColumnDef<*>>
        get() = this.inputs[0].columns

    /** The [FilterOnSubSelectPhysicalOperatorNode] requires all [ColumnDef]s used in the [KnnPredicate]. */
    override val requires: Array<ColumnDef<*>> = this.predicate.columns.toTypedArray()

    /** The [FilterOnSubSelectPhysicalOperatorNode] can only be executed if it doesn't contain any [ComparisonOperator.Binary.Match]. */
    override val executable: Boolean = this.predicate.atomics.none { it.operator is ComparisonOperator.Binary.Match } && this.inputs.all { it.executable }

    /** The output size of this [FilterOnSubSelectPhysicalOperatorNode]. TODO: Estimate selectivity of predicate. */
    override val outputSize: Long = this.inputs[0].outputSize

    /** The [Cost] of this [FilterOnSubSelectPhysicalOperatorNode]. */
    override val cost: Cost = Cost(cpu = this.inputs[0].outputSize * this.predicate.atomicCpuCost)

    /**
     * Returns a copy of this [FilterOnSubSelectPhysicalOperatorNode] and its input.
     *
     * @return Copy of this [FilterOnSubSelectPhysicalOperatorNode] and its input.
     */
    override fun copyWithInputs() = FilterOnSubSelectPhysicalOperatorNode(this.predicate, *this.inputs.map { it.copyWithInputs() }.toTypedArray())

    /**
     * Returns a copy of this [FilterOnSubSelectPhysicalOperatorNode] and its output.
     *
     * @param input The [OperatorNode] that should act as inputs.
     * @return Copy of this [FilterOnSubSelectPhysicalOperatorNode] and its output.
     */
    override fun copyWithOutput(input: OperatorNode.Physical?): OperatorNode.Physical {
        require(input != null) { "Input is required for copyWithOutput() on unary physical operator node." }
        val newInputs = this.inputs.map {
            if (it.groupId == input.groupId) {
                input
            } else {
                it.copyWithInputs()
            }
        }.toTypedArray()
        val filter = FilterOnSubSelectPhysicalOperatorNode(this.predicate, *newInputs)
        return (this.output?.copyWithOutput(filter) ?: filter)
    }

    /**
     *
     */
    override fun partition(p: Int): List<Physical> {
        throw UnsupportedOperationException("FilterOnSubSelectPhysicalOperatorNode's cannot be partitioned.")
    }

    /**
     * Converts this [FilterPhysicalOperatorNode] to a [FilterOnSubselectOperator].
     *
     * @param tx The [TransactionContext] used for execution.
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(tx: TransactionContext, ctx: QueryContext): Operator = FilterOnSubselectOperator(this.inputs[0].toOperator(tx, ctx), this.inputs.drop(1).map { it.toOperator(tx, ctx) }, this.predicate.bindValues(ctx.values))


    /**
     * Binds values from the provided [BindingContext] to this [FilterPhysicalOperatorNode]'s [BooleanPredicate].
     *
     * @param ctx The [BindingContext] used for value binding.
     */
    override fun bindValues(ctx: BindingContext<Value>): OperatorNode {
        this.predicate.bindValues(ctx)
        return super.bindValues(ctx) /* Important! */
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is FilterPhysicalOperatorNode) return false
        if (predicate != other.predicate) return false
        return true
    }

    override fun hashCode(): Int = this.predicate.hashCode()
}