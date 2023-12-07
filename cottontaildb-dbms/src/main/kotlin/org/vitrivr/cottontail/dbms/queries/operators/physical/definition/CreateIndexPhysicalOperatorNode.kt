package org.vitrivr.cottontail.dbms.queries.operators.physical.definition

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.execution.operators.definition.CreateIndexOperator
import org.vitrivr.cottontail.dbms.index.basic.IndexType
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * A [DataDefinitionPhysicalOperatorNode] used to create a new index.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class CreateIndexPhysicalOperatorNode(
    private val tx: CatalogueTx,
    private val indexName: Name.IndexName,
    private val type: IndexType,
    private val indexColumns: List<Name.ColumnName>,
    private val params: Map<String, String>) : DataDefinitionPhysicalOperatorNode("CreateIndex") {
    override fun copy() = CreateIndexPhysicalOperatorNode(this.tx, this.indexName, this.type, this.indexColumns, this.params)
    override fun toOperator(ctx: QueryContext) = CreateIndexOperator(this.tx, this.indexName, this.type, this.indexColumns, this.params, ctx)
    override fun digest(): Digest = this.hashCode().toLong()
}