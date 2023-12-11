package org.vitrivr.cottontail.dbms.queries.operators.physical.system

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.statistics.values.ValueStatistics

/**
 * An abstract implementation of a [NullaryPhysicalOperatorNode] that is used to execute system language statements.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class SystemPhysicalOperatorNode(override val name: String): NullaryPhysicalOperatorNode() {
    override val statistics: Map<ColumnDef<*>, ValueStatistics<*>> = emptyMap()
    override val outputSize: Long = 1
    override val groupId: GroupId = 0
    override val cost: Cost = Cost.ZERO
    override fun canBeExecuted(ctx: QueryContext): Boolean = true
}