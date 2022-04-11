package org.vitrivr.cottontail.dbms.queries.operators.physical.projection

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.projection.SelectDistinctProjectionOperator
import org.vitrivr.cottontail.dbms.execution.operators.projection.SelectProjectionOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.projection.Projection

/**
 * Formalizes a [UnaryPhysicalOperatorNode] operation in the Cottontail DB query execution engine.
 *
 * @author Ralph Gasser
 * @version 2.3.0
 */
class SelectProjectionPhysicalOperatorNode(input: Physical? = null, type: Projection, val fields: List<Name.ColumnName>): AbstractProjectionPhysicalOperatorNode(input, type) {

    init {
        /* Sanity check. */
        require(this.type in arrayOf(Projection.SELECT, Projection.SELECT_DISTINCT)) {
            "Projection of type ${this.type} cannot be used with instances of AggregatingProjectionLogicalNodeExpression."
        }
    }

    /** The name of this [SelectProjectionPhysicalOperatorNode]. */
    override val name: String
        get() = this.type.label()

    /** The [ColumnDef] generated by this [SelectProjectionPhysicalOperatorNode]. */
    override val columns: List<ColumnDef<*>>
        get() = this.input?.columns?.filter { c -> fields.any { f -> f == c.name }} ?: emptyList()

    /** The [ColumnDef] required by this [SelectProjectionPhysicalOperatorNode]. */
    override val requires: List<ColumnDef<*>>
        get() = this.columns

    /** The [Cost] of a [SelectProjectionPhysicalOperatorNode]. */
    override val cost: Cost
        get() = Cost.MEMORY_ACCESS * (this.outputSize * this.fields.size)

    init {
        /* Sanity check. */
        if (this.fields.isEmpty()) {
            throw QueryException.QuerySyntaxException("Projection of type $type must specify at least one column.")
        }
    }

    /**
     * Creates and returns a copy of this [SelectProjectionPhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [SelectProjectionPhysicalOperatorNode].
     */
    override fun copy() = SelectProjectionPhysicalOperatorNode(type = this.type, fields = this.fields)

    /**
     * Converts this [SelectProjectionPhysicalOperatorNode] to a [SelectProjectionOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator {
        val input = this.input ?: throw IllegalStateException("Cannot convert disconnected OperatorNode to Operator (node = $this)")
        return when (this.type) {
            Projection.SELECT -> SelectProjectionOperator(input.toOperator(ctx), this.fields)
            Projection.SELECT_DISTINCT -> SelectDistinctProjectionOperator(input.toOperator(ctx), this.fields, input.outputSize)
            else -> throw IllegalArgumentException("SelectProjectionPhysicalOperatorNode can only have type SELECT or SELECT_DISTINCT. This is a programmer's error!")
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is SelectProjectionPhysicalOperatorNode) return false
        if (this.type != other.type) return false
        if (this.fields != other.fields) return false
        return true
    }

    /**
     * Generates and returns a hash code for this [SelectProjectionPhysicalOperatorNode].
     */
    override fun hashCode(): Int {
        var result = this.type.hashCode()
        result = 31 * result + this.fields.hashCode()
        return result
    }
}