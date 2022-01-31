package org.vitrivr.cottontail.dbms.queries.operators.logical.management

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.management.DeleteOperator
import org.vitrivr.cottontail.dbms.queries.operators.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.management.DeletePhysicalOperatorNode

/**
 * A [DeleteLogicalOperatorNode] that formalizes a DELETE operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 2.2.1
 */
class DeleteLogicalOperatorNode(input: Logical? = null, val entity: EntityTx) : UnaryLogicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Delete"
    }

    /** The name of this [DeleteLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [DeleteLogicalOperatorNode] produces the columns defined in the [DeleteOperator] */
    override val columns: List<ColumnDef<*>> = DeleteOperator.COLUMNS

    /** The [DeleteLogicalOperatorNode] does not require any [ColumnDef]. */
    override val requires: List<ColumnDef<*>> = emptyList()

    /**
     * Creates and returns a copy of this [DeleteLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [DeleteLogicalOperatorNode].
     */
    override fun copy() = DeleteLogicalOperatorNode(entity = this.entity)

    /**
     * Returns a [DeletePhysicalOperatorNode] representation of this [DeleteLogicalOperatorNode]
     *
     * @return [DeletePhysicalOperatorNode]
     */
    override fun implement() = DeletePhysicalOperatorNode(this.input?.implement(), this.entity)

    override fun toString(): String = "${super.toString()}[${this.entity.dbo.name}]"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is DeleteLogicalOperatorNode) return false

        if (this.entity != other.entity) return false
        if (this.columns != other.columns) return false

        return true
    }

    override fun hashCode(): Int {
        var result = entity.hashCode()
        result = 31 * result + columns.hashCode()
        return result
    }
}