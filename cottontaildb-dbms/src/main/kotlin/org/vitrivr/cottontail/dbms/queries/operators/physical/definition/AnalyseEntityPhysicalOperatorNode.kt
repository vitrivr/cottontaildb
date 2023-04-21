package org.vitrivr.cottontail.dbms.queries.operators.physical.definition

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.operators.definition.AnalyseEntityOperator
import org.vitrivr.cottontail.dbms.execution.services.AutoAnalyzerService
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * A [DataDefinitionPhysicalOperatorNode] used to analyze an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class AnalyseEntityPhysicalOperatorNode(val tx: CatalogueTx, val entityName: Name.EntityName, val service: AutoAnalyzerService? = null): DataDefinitionPhysicalOperatorNode("AnalyseEntity") {
    override fun copy() = AnalyseEntityPhysicalOperatorNode(this.tx, this.entityName, this.service)
    override fun toOperator(ctx: QueryContext) = AnalyseEntityOperator(this.tx, this.entityName, this.service, ctx)
    override fun digest(): Digest = this.hashCode().toLong()
}

