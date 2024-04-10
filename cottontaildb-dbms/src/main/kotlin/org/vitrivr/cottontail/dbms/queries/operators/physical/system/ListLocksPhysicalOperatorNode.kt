package org.vitrivr.cottontail.dbms.queries.operators.physical.system

import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.dbms.execution.locking.LockManager
import org.vitrivr.cottontail.dbms.execution.operators.system.ListLocksOperator
import org.vitrivr.cottontail.dbms.general.DBO
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.ColumnSets

/**
 * A [SystemPhysicalOperatorNode] used to list all locks.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class ListLocksPhysicalOperatorNode(val context: QueryContext, val manager: LockManager<DBO>): SystemPhysicalOperatorNode("ListLocks") {
    override val groupId: GroupId = 0
    override val name: String = "ListLocks"
    override val outputSize: Long = 1L
    override val columns: List<Binding.Column> = ColumnSets.DDL_LOCKS_COLUMNS.map {
        this.context.bindings.bind(it, null)
    }
    override fun toOperator(ctx: QueryContext) = ListLocksOperator(this.manager, ctx)
    override fun copy() = ListLocksPhysicalOperatorNode(this.context, this.manager)

    /**
     * Generates and returns a [Digest] for this [ListLocksPhysicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest = this.hashCode() + 1L
}