package org.vitrivr.cottontail.dbms.queries.operators.physical.definition

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.operators.definition.TruncateEntityOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * A [DataDefinitionPhysicalOperatorNode] used to truncate an [Entity].
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class TruncateEntityPhysicalOperatorNode(override val context: QueryContext, val entity: Name.EntityName): DataDefinitionPhysicalOperatorNode("TruncateEntity") {
    override fun copy() = TruncateEntityPhysicalOperatorNode(this.context, this.entity)
    override fun toOperator(ctx: QueryContext) = TruncateEntityOperator(this.entity, ctx)
    override fun digest(): Digest = this.hashCode().toLong()
}