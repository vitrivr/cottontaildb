package org.vitrivr.cottontail.dbms.queries.operators.logical.projection

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.projection.SelectProjectionPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.projection.Projection

/**
 * A [AbstractProjectionLogicalOperatorOperator] that formalizes a [Projection.SELECT] operation in a Cottontail DB query plan.
 *
 * @author Ralph Gasser
 * @version 2.9.0
 */
class SelectProjectionLogicalOperatorNode(input: Logical, override val columns: List<Binding.Column>): AbstractProjectionLogicalOperatorOperator(input, Projection.SELECT) {

    /** The [ColumnDef] required by this [SelectProjectionLogicalOperatorNode]. */
    override val requires: List<Binding.Column> = this.columns

    init {
        /* Sanity check. */
        if (this.columns.isEmpty()) {
            throw QueryException.QuerySyntaxException("Projection of type $type must specify at least one column.")
        }
    }

    /**
     * Creates a copy of this [SelectProjectionLogicalOperatorNode].
     *
     * @param input The new input [OperatorNode.Logical]
     * @return Copy of this [SelectProjectionLogicalOperatorNode]
     */
    override fun copyWithNewInput(vararg input: Logical): SelectProjectionLogicalOperatorNode {
        require(input.size == 1) { "The input arity for SelectProjectionLogicalOperatorNode.copyWithNewInput() must be 1 but is ${input.size}. This is a programmer's error!"}
        return SelectProjectionLogicalOperatorNode(input = input[0], columns = this.columns)
    }

    /**
     * Returns a [SelectProjectionPhysicalOperatorNode] representation of this [SelectProjectionLogicalOperatorNode]
     *
     * @return [SelectProjectionPhysicalOperatorNode]
     */
    override fun implement(): Physical = SelectProjectionPhysicalOperatorNode(this.input.implement(), this.columns)

    /**
     * Generates and returns a [Digest] for this [SelectProjectionLogicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest {
        var result = Projection.SELECT.hashCode().toLong()
        result += 33L * result + this.columns.hashCode()
        return result
    }
}