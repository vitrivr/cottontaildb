package org.vitrivr.cottontail.dbms.queries.planning.nodes.logical.projection

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.planning.nodes.physical.projection.CountProjectionPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.projection.Projection

/**
 * A [UnaryLogicalOperatorNode] that represents a projection operation involving aggregate functions such as [Projection.COUNT].
 *
 * @author Ralph Gasser
 * @version 2.2.0
 */
class CountProjectionLogicalOperatorNode(input: Logical? = null, val alias: Name.ColumnName? = null) : AbstractProjectionLogicalOperatorOperator(input, Projection.COUNT) {

    /** The [ColumnDef] generated by this [CountProjectionLogicalOperatorNode]. */
    override val columns: List<ColumnDef<*>>
        get() {
            val name = this.alias ?: (this.input?.columns?.first()?.name?.entity()?.column(Projection.COUNT.label()) ?: Name.ColumnName(Projection.COUNT.label()))
            return listOf(ColumnDef(name, Types.Long, false))
        }

    /**
     * Creates and returns a copy of this [CountProjectionLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [CountProjectionLogicalOperatorNode].
     */
    override fun copy() = CountProjectionLogicalOperatorNode(alias = this.alias)

    /**
     * Returns a [CountProjectionPhysicalOperatorNode] representation of this [CountProjectionLogicalOperatorNode]
     *
     * @return [CountProjectionPhysicalOperatorNode]
     */
    override fun implement(): Physical = CountProjectionPhysicalOperatorNode(this.input?.implement(), this.alias)

    /**
     * Compares this [CountProjectionLogicalOperatorNode] to another object.
     *
     * @param other The other [Any] to compare this [CountProjectionLogicalOperatorNode] to.
     * @return True if other equals this, false otherwise.
     */
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is CountProjectionLogicalOperatorNode) return false
        if (this.type != other.type) return false
        if (this.alias != other.alias) return false
        return true
    }

    /**
     * Generates and returns a hash code for this [CountProjectionLogicalOperatorNode].
     */
    override fun hashCode(): Int {
        var result = type.hashCode()
        result = 31 * result + this.alias.hashCode()
        return result
    }
}