package org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection.AggregatingProjectionPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.projection.Projection
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.exceptions.QueryException

/**
 * A [UnaryLogicalOperatorNode] that represents a projection operation involving aggregate functions such as [Projection.MAX], [Projection.MIN] or [Projection.SUM].
 *
 * @author Ralph Gasser
 * @version 2.3.0
 */
class AggregatingProjectionLogicalOperatorNode(input: Logical? = null, type: Projection, val fields: List<Name.ColumnName>) : AbstractProjectionLogicalOperatorOperator(input, type) {

    /** The [ColumnDef] generated by this [AggregatingProjectionLogicalOperatorNode]. */
    override val columns: List<ColumnDef<*>>
        get() = this.fields.map {
            val col = this.input?.columns?.find { c -> c.name == it } ?: throw QueryException.QueryBindException("Column with name $it could not be found on input.")
            if (!col.type.numeric) throw QueryException.QueryBindException("Projection of type ${this.type} can only be applied to numeric column, which $col isn't.")
            col
        }

    /** The [ColumnDef] required by this [AggregatingProjectionLogicalOperatorNode]. */
    override val requires: List<ColumnDef<*>>
        get() = this.fields.map {
            this.input?.columns?.find { c -> c.name == it } ?: throw QueryException.QueryBindException("Column with name $it could not be found on input.")
        }

    init {
        /* Sanity check. */
        require(this.type in arrayOf(Projection.MIN, Projection.MAX, Projection.MAX, Projection.SUM)) {
            "Projection of type ${this.type} cannot be used with instances of AggregatingProjectionLogicalNodeExpression."
        }
    }

    /**
     * Creates and returns a copy of this [AggregatingProjectionLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [AggregatingProjectionLogicalOperatorNode].
     */
    override fun copy() = AggregatingProjectionLogicalOperatorNode(type = this.type, fields = this.fields)


    /**
     * Returns a [AggregatingProjectionPhysicalOperatorNode] representation of this [AggregatingProjectionLogicalOperatorNode]
     *
     * @return [AggregatingProjectionPhysicalOperatorNode]
     */
    override fun implement(): Physical = AggregatingProjectionPhysicalOperatorNode(this.input?.implement(), this.type, this.fields)
}