package org.vitrivr.cottontail.dbms.queries.operators.logical.projection

import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.projection.SelectDistinctProjectionPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.projection.Projection

/**
 * A [AbstractProjectionLogicalOperatorOperator] that formalizes a [Projection.SELECT_DISTINCT] operation in a Cottontail DB query plan.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class SelectDistinctProjectionLogicalOperatorNode(input: Logical, val fields: List<Pair<Name.ColumnName, Boolean>>, val config: Config): AbstractProjectionLogicalOperatorOperator(input, Projection.SELECT_DISTINCT) {
    /** The name of this [SelectDistinctProjectionLogicalOperatorNode]. */
    override val name: String = "SelectDistinct"

    /** The [ColumnDef] generated by this [SelectProjectionLogicalOperatorNode]. */
    override val columns: List<ColumnDef<*>> by lazy {
        this.input.columns.filter { c -> this.fields.any { f -> f.first == c.name }}
    }

    /** The [ColumnDef] required by this [SelectProjectionLogicalOperatorNode]. */
    override val requires: List<ColumnDef<*>>
        get() = this.columns

    init {
        /* Sanity check. */
        if (this.fields.isEmpty()) {
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
        return SelectDistinctProjectionLogicalOperatorNode(input = input[0], fields = this.fields, config = this.config)
    }

    /**
     * Returns a [SelectDistinctProjectionPhysicalOperatorNode] representation of this [SelectDistinctProjectionPhysicalOperatorNode]
     *
     * @return [SelectDistinctProjectionPhysicalOperatorNode]
     */
    override fun implement(): Physical = SelectDistinctProjectionPhysicalOperatorNode(this.input.implement(), this.fields, this.config)

    /**
     * Generates and returns a [Digest] for this [SelectDistinctProjectionLogicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest {
        var result = Projection.SELECT_DISTINCT.hashCode().toLong()
        result += 33L * result + this.fields.hashCode()
        return result
    }
}