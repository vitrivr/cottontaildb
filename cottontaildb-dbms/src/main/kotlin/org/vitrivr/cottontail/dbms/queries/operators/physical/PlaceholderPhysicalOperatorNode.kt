package org.vitrivr.cottontail.dbms.queries.operators.physical

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.statistics.columns.ValueStatistics

/**
 * This is a [NullaryPhysicalOperatorNode] that acts as placeholder for higher-level group inputs.
 *
 * Mainly used during decomposition of query plans into groups.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class PlaceholderPhysicalOperatorNode(override val groupId: GroupId, override val physicalColumns: List<ColumnDef<*>>, override val columns: List<ColumnDef<*>>): NullaryPhysicalOperatorNode() {
    override fun copy() = PlaceholderPhysicalOperatorNode(this.groupId, this.physicalColumns, this.columns)
    override val name = "Placeholder"
    override val statistics: Map<ColumnDef<*>, ValueStatistics<*>> = emptyMap()
    override val outputSize: Long = 0L
    override fun toOperator(ctx: QueryContext): Operator = throw UnsupportedOperationException("A PlaceholderPhysicalOperatorNode cannot be converted to an operator.")
    override val cost: Cost = Cost.ZERO
    override fun digest(): Digest = 0L
}