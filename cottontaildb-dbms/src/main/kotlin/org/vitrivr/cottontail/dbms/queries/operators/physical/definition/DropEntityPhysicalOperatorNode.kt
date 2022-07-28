package org.vitrivr.cottontail.dbms.queries.operators.physical.definition

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.operators.definition.DropEntityOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * A [DataDefinitionPhysicalOperatorNode] used to drop an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class DropEntityPhysicalOperatorNode(val tx: CatalogueTx, val entityName: Name.EntityName): DataDefinitionPhysicalOperatorNode("DropEntity") {
    override fun copy() = DropEntityPhysicalOperatorNode(this.tx, this.entityName)
    override fun toOperator(ctx: QueryContext) = DropEntityOperator(this.tx, this.entityName, ctx)
}