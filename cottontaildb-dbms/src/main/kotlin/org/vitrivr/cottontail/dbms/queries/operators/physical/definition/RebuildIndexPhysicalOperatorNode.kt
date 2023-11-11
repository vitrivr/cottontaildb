package org.vitrivr.cottontail.dbms.queries.operators.physical.definition

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.dbms.execution.operators.definition.RebuildIndexOperator
import org.vitrivr.cottontail.dbms.execution.services.AutoRebuilderService
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * A [DataDefinitionPhysicalOperatorNode] used to rebuild an [Index].
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class RebuildIndexPhysicalOperatorNode(override val context: QueryContext, val index: Name.IndexName, val service: AutoRebuilderService? = null): DataDefinitionPhysicalOperatorNode("RebuildIndex") {
    override fun copy() = RebuildIndexPhysicalOperatorNode(this.context, this.index, this.service)
    override fun toOperator(ctx: QueryContext) = RebuildIndexOperator(this.index, service, ctx)
    override fun digest(): Digest = this.hashCode().toLong()
}