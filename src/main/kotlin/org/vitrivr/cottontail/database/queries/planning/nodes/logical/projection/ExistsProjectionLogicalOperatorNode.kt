package org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection.ExistsProjectionPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.projection.Projection
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Type

/**
 * A [UnaryLogicalOperatorNode] that represents a projection operation involving aggregate functions such as [Projection.EXISTS].
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
class ExistsProjectionLogicalOperatorNode(input: Logical? = null, fields: List<Pair<Name.ColumnName, Name.ColumnName?>>) : AbstractProjectionLogicalOperatorOperator(input, Projection.EXISTS, fields) {

    /** The [ColumnDef] generated by this [ExistsProjectionLogicalOperatorNode]. */
    override val columns: Array<ColumnDef<*>>
        get() {
            val alias = fields.first().second
            val name = alias ?: (this.input?.columns?.first()?.name?.entity()?.column(Projection.EXISTS.label()) ?: Name.ColumnName(Projection.EXISTS.label()))
            return arrayOf(ColumnDef(name, Type.Boolean, false))
        }

    /** The [ColumnDef] required by this [ExistsProjectionLogicalOperatorNode]. */
    override val requires: Array<ColumnDef<*>> = emptyArray()

    /**
     * Creates and returns a copy of this [ExistsProjectionLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [ExistsProjectionLogicalOperatorNode].
     */
    override fun copy() = ExistsProjectionLogicalOperatorNode(fields = this.fields)

    /**
     * Returns a [ExistsProjectionPhysicalOperatorNode] representation of this [ExistsProjectionLogicalOperatorNode]
     *
     * @return [ExistsProjectionPhysicalOperatorNode]
     */
    override fun implement(): Physical = ExistsProjectionPhysicalOperatorNode(this.input?.implement(), this.fields)
}