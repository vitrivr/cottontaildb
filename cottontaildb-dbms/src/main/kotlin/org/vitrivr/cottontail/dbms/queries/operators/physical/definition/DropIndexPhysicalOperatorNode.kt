package org.vitrivr.cottontail.dbms.queries.operators.physical.definition

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.execution.operators.definition.DropIndexOperator
import org.vitrivr.cottontail.dbms.index.Index
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * A [DataDefinitionPhysicalOperatorNode] used to drop an [Index].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class DropIndexPhysicalOperatorNode(val tx: CatalogueTx, val indexName: Name.IndexName): DataDefinitionPhysicalOperatorNode("DropIndex") {
    override fun copy() = DropIndexPhysicalOperatorNode(this.tx, this.indexName)
    override fun toOperator(ctx: QueryContext) = DropIndexOperator(this.tx, this.indexName)
}