package org.vitrivr.cottontail.dbms.queries.operators.physical.projection

import org.vitrivr.cottontail.config.Config
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.execution.operators.projection.ExistsProjectionOperator
import org.vitrivr.cottontail.dbms.execution.operators.projection.SelectDistinctProjectionOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.projection.Projection

/**
 * Formalizes a [Projection.SELECT_DISTINCT] operation in a Cottontail DB query plan.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class SelectDistinctProjectionPhysicalOperatorNode(input: Physical, val fields: List<Pair<Name.ColumnName,Boolean>>, val config: Config): UnaryPhysicalOperatorNode(input) {
    /** The name of this [CountProjectionPhysicalOperatorNode]. */
    override val name: String
        get() = Projection.SELECT_DISTINCT.label()

    /** The [ColumnDef] generated by this [SelectDistinctProjectionPhysicalOperatorNode]. */
    override val columns: List<ColumnDef<*>> by lazy {
        this.input.columns.filter { c -> fields.any { f -> f.first == c.name }}
    }

    /** The [ColumnDef] required by this [SelectProjectionPhysicalOperatorNode]. */
    override val requires: List<ColumnDef<*>>
        get() = this.columns

    /** The [Cost] of a [SelectDistinctProjectionPhysicalOperatorNode]. */
    context(BindingContext, Tuple)
    override val cost: Cost
        get() = Cost.MEMORY_ACCESS * (this.outputSize * this.fields.size) + Cost.DISK_ACCESS_WRITE * (16 * this.fields.size * this.outputSize)

    init {
        /* Sanity check. */
        if (this.fields.isEmpty()) {
            throw QueryException.QuerySyntaxException("Projection of type ${Projection.SELECT_DISTINCT} must specify at least one column.")
        }
    }

    /**
     * Creates and returns a copy of this [SelectDistinctProjectionPhysicalOperatorNode] using the given parents as input.
     *
     * @param input The [OperatorNode.Physical]s that act as input.
     * @return Copy of this [SelectDistinctProjectionPhysicalOperatorNode].
     */
    override fun copyWithNewInput(vararg input: Physical): SelectDistinctProjectionPhysicalOperatorNode {
        require(input.size == 1) { "The input arity for SelectDistinctProjectionPhysicalOperatorNode.copyWithNewInput() must be 1 but is ${input.size}. This is a programmer's error!"}
        return SelectDistinctProjectionPhysicalOperatorNode(input = input[0], fields = this.fields, config = this.config)
    }

    /**
     * Converts this [SelectDistinctProjectionPhysicalOperatorNode] to a [SelectDistinctProjectionPhysicalOperatorNode].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext) = SelectDistinctProjectionOperator(this.input.toOperator(ctx), this.fields, ctx)

    /** Generates and returns a [String] representation of this [ExistsProjectionOperator]. */
    override fun toString() = "${super.toString()}[${this.columns.joinToString(",") { it.name.toString() }}]"

    /**
     *
     */
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is SelectDistinctProjectionPhysicalOperatorNode) return false
        if (this.fields != other.fields) return false
        return true
    }

    /**
     * Generates and returns a [Digest] for this [SelectDistinctProjectionPhysicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest {
        var result = Projection.SELECT_DISTINCT.hashCode().toLong()
        result += 31L * result + this.fields.hashCode()
        return result
    }
}