package org.vitrivr.cottontail.dbms.queries.operators.physical.projection

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.execution.operators.projection.SelectProjectionOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.projection.Projection

/**
 * A [UnaryPhysicalOperatorNode] that formalizes a [Projection.SELECT] operation in a Cottontail DB query plan.
 *
 * @author Ralph Gasser
 * @version 2.9.0
 */
class SelectProjectionPhysicalOperatorNode(input: Physical, override val columns: List<Binding.Column>): UnaryPhysicalOperatorNode(input) {

    /** The name of this [SelectProjectionPhysicalOperatorNode]. */
    override val name: String
        get() = Projection.SELECT.label()


    /** The [ColumnDef] required by this [SelectProjectionPhysicalOperatorNode]. */
    override val requires: List<Binding.Column> = this.columns

    /** The [Cost] of a [SelectProjectionPhysicalOperatorNode]. */
    context(BindingContext, Tuple)    override val cost: Cost
        get() = Cost.MEMORY_ACCESS * (this.outputSize * this.columns.size)

    init {
        /* Sanity check. */
        if (this.columns.isEmpty()) {
            throw QueryException.QuerySyntaxException("Projection of type ${Projection.SELECT} must specify at least one column.")
        }
    }

    /**
     * Creates and returns a copy of this [SelectProjectionPhysicalOperatorNode] using the given parents as input.
     *
     * @param input The [OperatorNode.Physical]s that act as input.
     * @return Copy of this [SelectProjectionPhysicalOperatorNode].
     */
    override fun copyWithNewInput(vararg input: Physical): SelectProjectionPhysicalOperatorNode {
        require(input.size == 1) { "The input arity for SelectProjectionPhysicalOperatorNode.copyWithNewInput() must be 1 but is ${input.size}. This is a programmer's error!"}
        return SelectProjectionPhysicalOperatorNode(input = input[0], columns = this.columns)
    }

    /**
     * Converts this [SelectProjectionPhysicalOperatorNode] to a [SelectProjectionOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext) = SelectProjectionOperator(this.input.toOperator(ctx), this.columns, ctx)

    /** Generates and returns a [String] representation of this [SelectProjectionPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}[${this.columns.joinToString(",") { it.column.name.toString() }}]"

    /**
     * Generates and returns a [Digest] for this [SelectProjectionPhysicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest {
        var result = Projection.SELECT.hashCode().toLong()
        result += 31L * result + this.columns.hashCode()
        return result
    }
}