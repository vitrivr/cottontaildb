package org.vitrivr.cottontail.database.queries.planning.nodes.logical.management

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.GroupId
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.binding.Binding
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.NullaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.management.InsertPhysicalOperatorNode
import org.vitrivr.cottontail.execution.operators.management.InsertOperator
import org.vitrivr.cottontail.model.basics.Record

/**
 * A [InsertLogicalOperatorNode] that formalizes a INSERT operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class InsertLogicalOperatorNode(override val groupId: GroupId, val entity: Entity, val records: MutableList<Binding<Record>>) : NullaryLogicalOperatorNode() {
    /** The [InsertLogicalOperatorNode] produces the columns defined in the [InsertOperator] */
    override val columns: Array<ColumnDef<*>> = InsertOperator.COLUMNS

    /**
     * Returns a copy of this [DeleteLogicalOperatorNode]
     *
     * @return Copy of this [DeleteLogicalOperatorNode]
     */
    override fun copyWithInputs(): InsertLogicalOperatorNode = InsertLogicalOperatorNode(this.groupId, this.entity, this.records)

    /**
     * Returns a copy of this [InsertLogicalOperatorNode] and its output.
     *
     * @param inputs The [OperatorNode.Logical] that should act as inputs.
     * @return Copy of this [InsertLogicalOperatorNode] and its output.
     */
    override fun copyWithOutput(vararg inputs: OperatorNode.Logical): OperatorNode.Logical {
        require(inputs.isEmpty()) { "No input is allowed for nullary operators." }
        val insert = InsertLogicalOperatorNode(this.groupId, this.entity, this.records)
        return (this.output?.copyWithOutput(insert) ?: insert)
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