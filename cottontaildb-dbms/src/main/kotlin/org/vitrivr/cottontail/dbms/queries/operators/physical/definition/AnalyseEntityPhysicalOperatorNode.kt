package org.vitrivr.cottontail.dbms.queries.operators.physical.definition

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.operators.definition.AnalyseEntityOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * A [DataDefinitionPhysicalOperatorNode] used to analyze an [Entity].
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class AnalyseEntityPhysicalOperatorNode(override val context: QueryContext, val entity: Name.EntityName): DataDefinitionPhysicalOperatorNode("AnalyseEntity") {
    override fun copy() = AnalyseEntityPhysicalOperatorNode(this.context,this.entity)
    override fun toOperator(ctx: QueryContext) = AnalyseEntityOperator(this.entity, ctx)
    override fun digest(): Digest = this.hashCode().toLong()
}

