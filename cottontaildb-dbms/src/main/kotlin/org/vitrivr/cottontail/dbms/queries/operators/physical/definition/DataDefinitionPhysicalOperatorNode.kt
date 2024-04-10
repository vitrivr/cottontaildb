package org.vitrivr.cottontail.dbms.queries.operators.physical.definition

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.statistics.values.ValueStatistics

/**
 * An abstract implementation of a [NullaryPhysicalOperatorNode] that is used to execute data definition language statements.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class DataDefinitionPhysicalOperatorNode(override val name: String, val context: QueryContext, columns: List<ColumnDef<*>>): NullaryPhysicalOperatorNode() {
    override val statistics: Map<ColumnDef<*>, ValueStatistics<*>> = emptyMap()
    override val outputSize: Long = 1
    override val groupId: GroupId = 0
    override val columns: List<Binding.Column> = columns.map {
        this.context.bindings.bind(it, null)
    }
    override val cost: Cost = Cost.ZERO
    override fun canBeExecuted(ctx: QueryContext): Boolean = true
}