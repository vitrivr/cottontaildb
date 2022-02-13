package org.vitrivr.cottontail.dbms.queries.operators.logical.management

import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.management.InsertOperator
import org.vitrivr.cottontail.dbms.queries.operators.logical.NullaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.management.InsertPhysicalOperatorNode

/**
 * A [InsertLogicalOperatorNode] that formalizes a INSERT operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 2.5.0
 */
class InsertLogicalOperatorNode(override val groupId: GroupId, val entity: EntityTx, val records: MutableList<Record>) : NullaryLogicalOperatorNode() {

    companion object {
        private const val NODE_NAME = "Insert"
    }

    /** The name of this [InsertLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The physical [ColumnDef] accessed by the [InsertLogicalOperatorNode]. */
    override val physicalColumns: List<ColumnDef<*>> = this.entity.listColumns()

    /** The [InsertLogicalOperatorNode] produces the columns defined in the [InsertOperator] */
    override val columns: List<ColumnDef<*>> = InsertOperator.COLUMNS

    /**
     * Creates and returns a copy of this [InsertLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [InsertLogicalOperatorNode].
     */
    override fun copy() = InsertLogicalOperatorNode(this.groupId, this.entity, this.records)

    /**
     *
     */
    override fun bind(context: BindingContext) {
        /* No op. */
    }

    /**
     * Returns a [InsertPhysicalOperatorNode] representation of this [InsertLogicalOperatorNode]
     *
     * @return [InsertPhysicalOperatorNode]
     */
    override fun implement() = InsertPhysicalOperatorNode(this.groupId, this.entity, this.records)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is InsertLogicalOperatorNode) return false

        if (entity != other.entity) return false
        if (records != other.records) return false

        return true
    }

    override fun hashCode(): Int {
        var result = entity.hashCode()
        result = 31 * result + records.hashCode()
        return result
    }
}