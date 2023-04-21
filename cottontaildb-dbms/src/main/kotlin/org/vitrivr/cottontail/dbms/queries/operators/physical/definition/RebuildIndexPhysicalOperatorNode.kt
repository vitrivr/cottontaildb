package org.vitrivr.cottontail.dbms.queries.operators.physical.definition

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.execution.operators.definition.RebuildIndexOperator
import org.vitrivr.cottontail.dbms.execution.services.AutoRebuilderService
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * A [DataDefinitionPhysicalOperatorNode] used to rebuild an [Index].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class RebuildIndexPhysicalOperatorNode(val tx: CatalogueTx, val indexName: Name.IndexName, val service: AutoRebuilderService? = null): DataDefinitionPhysicalOperatorNode("RebuildIndex") {
    override fun copy() = RebuildIndexPhysicalOperatorNode(this.tx, this.indexName, this.service)
    override fun toOperator(ctx: QueryContext) = RebuildIndexOperator(this.tx, this.indexName, service, ctx)
    override fun digest(): Digest = this.hashCode().toLong()
}