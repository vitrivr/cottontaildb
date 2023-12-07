package org.vitrivr.cottontail.dbms.queries.operators.logical.projection

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.projection.ExistsProjectionPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.projection.Projection

/**
 * A [UnaryLogicalOperatorNode] that represents a projection operation involving aggregate functions such as [Projection.EXISTS].
 *
 * @author Ralph Gasser
 * @version 2.4.0
 */
class ExistsProjectionLogicalOperatorNode(input: Logical, val out: Binding.Column) : AbstractProjectionLogicalOperatorOperator(input, Projection.EXISTS) {

    /** The [ColumnDef] generated by this [ExistsProjectionLogicalOperatorNode]. */
    override val columns: List<ColumnDef<*>>
        get() = listOf(this.out.column)


    /**
     * Creates a copy of this [ExistsProjectionLogicalOperatorNode].
     *
     * @param input The new input [OperatorNode.Logical]
     * @return Copy of this [ExistsProjectionLogicalOperatorNode]
     */
    override fun copyWithNewInput(vararg input: Logical): ExistsProjectionLogicalOperatorNode {
        require(input.size == 1) { "The input arity for SelectProjectionLogicalOperatorNode.copyWithNewInput() must be 1 but is ${input.size}. This is a programmer's error!"}
        return ExistsProjectionLogicalOperatorNode(input = input[0], out = this.out.copy())
    }

    /**
     * Returns a [ExistsProjectionPhysicalOperatorNode] representation of this [ExistsProjectionLogicalOperatorNode]
     *
     * @return [ExistsProjectionPhysicalOperatorNode]
     */
    override fun implement(): Physical = ExistsProjectionPhysicalOperatorNode(this.input.implement(), this.out)

    /**
     * Generates and returns a [Digest] for this [ExistsProjectionLogicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest {
        var result = Projection.EXISTS.hashCode().toLong()
        result += 33L * result + this.out.hashCode()
        return result
    }
}