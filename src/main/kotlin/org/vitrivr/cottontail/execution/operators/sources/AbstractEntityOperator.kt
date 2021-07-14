package org.vitrivr.cottontail.execution.operators.sources

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.queries.GroupId
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.model.basics.Name

/**
 * An abstract [Operator.SourceOperator] that accesses an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
abstract class AbstractEntityOperator(groupId: GroupId, protected val entity: EntityTx, val fetch: List<Pair<Name.ColumnName,ColumnDef<*>>>) : Operator.SourceOperator(groupId) {
    override val columns: List<ColumnDef<*>> = this.fetch.map { it.second.copy(name = it.first) }
}