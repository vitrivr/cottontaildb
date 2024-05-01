package org.vitrivr.cottontail.dbms.queries.operators.logical.projection

import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.projection.SelectDistinctProjectionPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.projection.Projection

/**
 * A [AbstractProjectionLogicalOperatorOperator] that formalizes a [Projection.SELECT_DISTINCT] operation in a Cottontail DB query plan.
 *
 * @author Ralph Gasser
 * @version 2.9.0
 */
class SelectDistinctProjectionLogicalOperatorNode(input: Logical, override val columns: List<Binding.Column>, val config: Config): AbstractProjectionLogicalOperatorOperator(input, Projection.SELECT_DISTINCT) {
    /** The name of this [SelectDistinctProjectionLogicalOperatorNode]. */
    override val name: String = "SelectDistinct"

    /** The [ColumnDef] required by this [SelectProjectionLogicalOperatorNode]. */
    override val requires: List<Binding.Column> = this.columns

    init {
        /* Sanity check. */
        if (this.columns.isEmpty()) {
            throw QueryException.QuerySyntaxException("Projection of type $type must specify at least one column.")
        }
    }

    /**
     * Creates a copy of this [SelectDistinctProjectionLogicalOperatorNode].
     *
     * @param input The new input [OperatorNode.Logical]
     * @return Copy of this [SelectDistinctProjectionLogicalOperatorNode]
     */
    override fun copyWithNewInput(vararg input: Logical): SelectDistinctProjectionLogicalOperatorNode {
        require(input.size == 1) { "The input arity for SelectDistinctProjectionLogicalOperatorNode.copyWithNewInput() must be 1 but is ${input.size}. This is a programmer's error!"}
        return SelectDistinctProjectionLogicalOperatorNode(input = input[0], columns = this.columns, config = this.config)
    }

    /**
     * Returns a [SelectDistinctProjectionPhysicalOperatorNode] representation of this [SelectDistinctProjectionPhysicalOperatorNode]
     *
     * @return [SelectDistinctProjectionPhysicalOperatorNode]
     */
    override fun implement(): Physical = SelectDistinctProjectionPhysicalOperatorNode(this.input.implement(), this.columns, this.config)

    /**
     * Generates and returns a [Digest] for this [SelectDistinctProjectionLogicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest {
        var result = Projection.SELECT_DISTINCT.hashCode().toLong()
        result += 33L * result + this.columns.hashCode()
        return result
    }
}