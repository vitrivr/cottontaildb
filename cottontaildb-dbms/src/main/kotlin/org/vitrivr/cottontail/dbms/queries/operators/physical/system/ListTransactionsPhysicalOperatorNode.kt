package org.vitrivr.cottontail.dbms.queries.operators.physical.system

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.dbms.execution.operators.system.ListTransactionsOperator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionManager
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.ColumnSets
import org.vitrivr.cottontail.dbms.queries.operators.basics.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.statistics.columns.ValueStatistics

/**
 * A [NullaryPhysicalOperatorNode] used to list all transactions.
 *
 * @author Ralph Gasser
 * @version 1.0.0.
 */
class ListTransactionsPhysicalOperatorNode(val manager: TransactionManager): NullaryPhysicalOperatorNode() {
    override val groupId: GroupId = 0
    override val name: String = "ListTransactions"
    override val outputSize: Long = 1L
    override val statistics: Map<ColumnDef<*>, ValueStatistics<*>>
        get() = emptyMap()
    override val columns: List<ColumnDef<*>>
        get() = ColumnSets.DDL_LOCKS_COLUMNS
    override val physicalColumns: List<ColumnDef<*>>
        get() = emptyList()
    override val cost: Cost = Cost.ZERO
    override fun toOperator(ctx: QueryContext) = ListTransactionsOperator(this.manager, ctx)
    override fun copy() = ListTransactionsPhysicalOperatorNode(this.manager)
}