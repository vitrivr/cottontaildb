package org.vitrivr.cottontail.dbms.queries.operators.logical.projection

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.projection.AggregatingProjectionPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.projection.Projection

/**
 * A [UnaryLogicalOperatorNode] that represents a projection operation involving aggregate functions such as [Projection.MAX], [Projection.MIN] or [Projection.SUM].
 *
 * @author Ralph Gasser
 * @version 2.9.0
 */
class AggregatingProjectionLogicalOperatorNode(input: Logical, type: Projection, override val columns: List<Binding.Column>) : AbstractProjectionLogicalOperatorOperator(input, type) {

    companion object {
        private val SUPPORTED_OPERATORS = setOf(Projection.MIN, Projection.MAX, Projection.MAX, Projection.SUM, Projection.MEAN)
    }

    init {
        require(this.columns.all { it.type is Types.Numeric }) { "Projection of type ${this.type} can only be applied to numeric column, which $columns isn't." }
    }

    /** The [ColumnDef] required by this [AggregatingProjectionLogicalOperatorNode]. */
    override val requires: List<Binding.Column> = this.columns

    init {
        /* Sanity check. */
        require(this.type in SUPPORTED_OPERATORS) { "Projection of type ${this.type} cannot be used with instances of AggregatingProjectionLogicalNodeExpression. This is a programmer's error!" }
    }

    /**
     * Creates a copy of this [AggregatingProjectionLogicalOperatorNode].
     *
     * @param input The new input [OperatorNode.Logical]
     * @return Copy of this [AggregatingProjectionLogicalOperatorNode]
     */
    override fun copyWithNewInput(vararg input: Logical): AggregatingProjectionLogicalOperatorNode {
        require(input.size == 1) { "The input arity for AggregatingProjectionLogicalOperatorNode.copyWithNewInput() must be 1 but is ${input.size}. This is a programmer's error!"}
        return AggregatingProjectionLogicalOperatorNode(input = input[0], type = this.type, columns = this.columns)
    }

    /**
     * Returns a [AggregatingProjectionPhysicalOperatorNode] representation of this [AggregatingProjectionLogicalOperatorNode]
     *
     * @return [AggregatingProjectionPhysicalOperatorNode]
     */
    override fun implement(): Physical = AggregatingProjectionPhysicalOperatorNode(this.input.implement(), this.type, this.columns)

    /**
     * Generates and returns a [Digest] for this [AggregatingProjectionLogicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest {
        var result = this.type.hashCode().toLong()
        result += 33L * result + this.columns.hashCode()
        return result
    }
}