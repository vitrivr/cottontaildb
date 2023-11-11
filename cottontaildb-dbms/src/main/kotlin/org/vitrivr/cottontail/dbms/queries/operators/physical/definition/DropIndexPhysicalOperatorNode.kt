package org.vitrivr.cottontail.dbms.queries.operators.physical.definition

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.dbms.execution.operators.definition.DropIndexOperator
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * A [DataDefinitionPhysicalOperatorNode] used to drop an [Index].
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class DropIndexPhysicalOperatorNode(override val context: QueryContext, val index: Name.IndexName): DataDefinitionPhysicalOperatorNode("DropIndex") {
    override fun copy() = DropIndexPhysicalOperatorNode(this.context, this.index)
    override fun toOperator(ctx: QueryContext) = DropIndexOperator(this.index, ctx)
    override fun digest(): Digest = this.hashCode().toLong()
}