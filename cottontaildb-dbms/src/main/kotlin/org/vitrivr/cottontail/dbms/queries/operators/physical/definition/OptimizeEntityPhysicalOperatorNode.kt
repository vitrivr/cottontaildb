package org.vitrivr.cottontail.dbms.queries.operators.physical.definition

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.operators.definition.AnalyseEntityOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * A [DataDefinitionPhysicalOperatorNode] used to optimize an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class OptimizeEntityPhysicalOperatorNode(val tx: CatalogueTx, val entityName: Name.EntityName): DataDefinitionPhysicalOperatorNode("OptimizeEntity") {
    override fun copy() = OptimizeEntityPhysicalOperatorNode(this.tx, this.entityName)
    override fun toOperator(ctx: QueryContext) = AnalyseEntityOperator(this.tx, this.entityName)
}