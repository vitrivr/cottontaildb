package org.vitrivr.cottontail.dbms.queries.operators.physical.definition

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.operators.definition.TruncateEntityOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * A [DataDefinitionPhysicalOperatorNode] used to truncate an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class TruncateEntityPhysicalOperatorNode(val tx: CatalogueTx, val entityName: Name.EntityName): DataDefinitionPhysicalOperatorNode("TruncateEntity") {
    override fun copy() = TruncateEntityPhysicalOperatorNode(this.tx, this.entityName)
    override fun toOperator(ctx: QueryContext) = TruncateEntityOperator(this.tx, this.entityName, ctx)
}