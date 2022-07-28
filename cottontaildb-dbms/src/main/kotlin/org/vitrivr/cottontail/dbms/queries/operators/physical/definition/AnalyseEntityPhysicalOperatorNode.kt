package org.vitrivr.cottontail.dbms.queries.operators.physical.definition

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.operators.definition.AnalyseEntityOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * A [DataDefinitionPhysicalOperatorNode] used to analyze an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class AnalyseEntityPhysicalOperatorNode(val tx: CatalogueTx, val entityName: Name.EntityName): DataDefinitionPhysicalOperatorNode("AnalyseEntity") {
    override fun copy() = AnalyseEntityPhysicalOperatorNode(this.tx, this.entityName)
    override fun toOperator(ctx: QueryContext) = AnalyseEntityOperator(this.tx, this.entityName, ctx)
}

