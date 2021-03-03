package org.vitrivr.cottontail.database.queries.planning.nodes.logical.predicates

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.BinaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.NAryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.predicates.FilterOnSubSelectPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.predicates.bool.BooleanPredicate

/**
 * A [BinaryLogicalOperatorNode] that formalizes filtering using some [BooleanPredicate].
 *
 * As opposed to [FilterLogicalOperatorNode], the [FilterOnSubSelectLogicalOperatorNode] depends on
 * the execution of one or many sub-queries.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class FilterOnSubSelectLogicalOperatorNode(val predicate: BooleanPredicate, vararg inputs: OperatorNode.Logical) : NAryLogicalOperatorNode(*inputs) {

    /** [ColumnDef] produced by [FilterOnSubSelectLogicalOperatorNode] are determined by the left [OperatorNode.Logical]. */
    override val columns: Array<ColumnDef<*>> = this.inputs[0].columns

    /** The [FilterOnSubSelectLogicalOperatorNode] requires all [ColumnDef]s used in the [BooleanPredicate]. */
    override val requires: Array<ColumnDef<*>>
        get() = this.predicate.columns.toTypedArray()

    /**
     * Returns a copy of this [FilterOnSubSelectLogicalOperatorNode] and its input.
     *
     * @return Copy of this [FilterOnSubSelectLogicalOperatorNode] and its input.
     */
    override fun copyWithInputs() = FilterOnSubSelectLogicalOperatorNode(this.predicate, *this.inputs.map { it.copyWithInputs() }.toTypedArray())

    /**
     * Returns a copy of this [FilterOnSubSelectLogicalOperatorNode] and its output.
     *
     * @param input The [OperatorNode.Logical] that should act as inputs.
     * @return Copy of this [FilterOnSubSelectLogicalOperatorNode] and its output.
     */
    override fun copyWithOutput(input: OperatorNode.Logical?): OperatorNode.Logical {
        require(input != null) { "Input is required for unary logical operator node." }
        val newInputs = this.inputs.map {
            if (it.groupId == input.groupId) {
                input
            } else {
                it.copyWithInputs()
            }
        }.toTypedArray()
        val filter = FilterOnSubSelectLogicalOperatorNode(this.predicate, *newInputs)
        return (this.output?.copyWithOutput(filter) ?: filter)
    }

    /**
     * Returns a [FilterOnSubSelectPhysicalOperatorNode] representation of this [FilterOnSubSelectLogicalOperatorNode]
     *
     * @return [FilterOnSubSelectPhysicalOperatorNode]
     */
    override fun implement() = FilterOnSubSelectPhysicalOperatorNode(this.predicate, *this.inputs.map { it.implement() }.toTypedArray())

    /** Generates and returns a [String] representation of this [FilterLogicalOperatorNode]. */
    override fun toString() = "${super.toString()}(${this.predicate})"
}