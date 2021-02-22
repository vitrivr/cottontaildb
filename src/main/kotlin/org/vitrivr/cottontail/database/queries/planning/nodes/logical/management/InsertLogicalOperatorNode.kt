package org.vitrivr.cottontail.database.queries.planning.nodes.logical.management

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.binding.RecordBinding
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.management.InsertPhysicalOperatorNode
import org.vitrivr.cottontail.execution.operators.management.InsertOperator

/**
 * A [InsertLogicalOperatorNode] that formalizes a INSERT operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class InsertLogicalOperatorNode(val entity: Entity, val records: MutableList<RecordBinding>) : UnaryLogicalOperatorNode() {
    /** The [InsertLogicalOperatorNode] produces the columns defined in the [InsertOperator] */
    override val columns: Array<ColumnDef<*>> = InsertOperator.COLUMNS

    /**
     * Returns a copy of this [DeleteLogicalOperatorNode]
     *
     * @return Copy of this [DeleteLogicalOperatorNode]
     */
    override fun copy(): InsertLogicalOperatorNode = InsertLogicalOperatorNode(this.entity, this.records)

    /**
     * Returns a [InsertPhysicalOperatorNode] representation of this [InsertLogicalOperatorNode]
     *
     * @return [InsertPhysicalOperatorNode]
     */
    override fun implement() = InsertPhysicalOperatorNode(this.entity, this.records)

    /**
     * Calculates and returns the digest for this [InsertLogicalOperatorNode].
     *
     * @return Digest for this [InsertLogicalOperatorNode]
     */
    override fun digest(): Long {
        var result = 31L * super.digest()
        result = 31 * result + this.entity.hashCode()
        result = 31 * result + this.records.hashCode()
        return result
    }
}