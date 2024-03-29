package org.vitrivr.cottontail.dbms.queries.operators.physical.system

import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.dbms.execution.operators.system.ListTransactionsOperator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionManager
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.ColumnSets
import org.vitrivr.cottontail.dbms.queries.operators.basics.NullaryPhysicalOperatorNode

/**
 * A [NullaryPhysicalOperatorNode] used to list all transactions.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class ListTransactionsPhysicalOperatorNode(val context: QueryContext, val manager: TransactionManager): SystemPhysicalOperatorNode("ListTransactions") {
    override val groupId: GroupId = 0
    override val outputSize: Long = 1L
    override val columns: List<Binding.Column> = ColumnSets.DDL_LOCKS_COLUMNS.map {
        this.context.bindings.bind(it, null)
    }

    override fun toOperator(ctx: QueryContext) = ListTransactionsOperator(this.manager, ctx)
    override fun copy() = ListTransactionsPhysicalOperatorNode(this.context, this.manager)

    /**
     * Generates and returns a [Digest] for this [ListTransactionsPhysicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest = this.hashCode() + 1L
}