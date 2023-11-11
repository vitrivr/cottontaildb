package org.vitrivr.cottontail.dbms.queries.operators.physical.definition

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.dbms.execution.operators.definition.CreateIndexOperator
import org.vitrivr.cottontail.dbms.index.basic.IndexType
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * A [DataDefinitionPhysicalOperatorNode] used to create a new index.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class CreateIndexPhysicalOperatorNode(override val context: QueryContext, val index: Name.IndexName, val type: IndexType, val indexColumns: List<Name.ColumnName>, val params: Map<String, String>) : DataDefinitionPhysicalOperatorNode("CreateIndex") {
    override fun copy() = CreateIndexPhysicalOperatorNode( this.context, this.index, this.type, this.indexColumns, this.params)
    override fun toOperator(ctx: QueryContext) = CreateIndexOperator(this.index, this.type, this.indexColumns, this.params, ctx)
    override fun digest(): Digest = this.hashCode().toLong()
}