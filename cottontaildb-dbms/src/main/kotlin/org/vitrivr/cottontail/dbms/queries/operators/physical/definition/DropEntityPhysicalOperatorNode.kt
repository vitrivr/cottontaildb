package org.vitrivr.cottontail.dbms.queries.operators.physical.definition

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.operators.definition.DropEntityOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * A [DataDefinitionPhysicalOperatorNode] used to drop an [Entity].
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class DropEntityPhysicalOperatorNode(override val context: QueryContext, val entity: Name.EntityName): DataDefinitionPhysicalOperatorNode("DropEntity") {
    override fun copy() = DropEntityPhysicalOperatorNode(this.context, this.entity)
    override fun toOperator(ctx: QueryContext) = DropEntityOperator(this.entity, ctx)
    override fun digest(): Digest = this.hashCode().toLong()
}