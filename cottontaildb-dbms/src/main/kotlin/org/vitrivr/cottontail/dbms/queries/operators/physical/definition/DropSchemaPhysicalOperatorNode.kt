package org.vitrivr.cottontail.dbms.queries.operators.physical.definition

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.execution.operators.definition.DropSchemaOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.ColumnSets
import org.vitrivr.cottontail.dbms.schema.Schema

/**
 * A [DataDefinitionPhysicalOperatorNode] used to drop a [Schema].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class DropSchemaPhysicalOperatorNode(
    val tx: CatalogueTx,
    val schemaName: Name.SchemaName,
    context: QueryContext
): DataDefinitionPhysicalOperatorNode("DropSchema", context, ColumnSets.DDL_STATUS_COLUMNS) {
    override fun copy() = DropSchemaPhysicalOperatorNode(this.tx, this.schemaName, this.context)
    override fun toOperator(ctx: QueryContext) = DropSchemaOperator(this.tx, this.schemaName, ctx)
    override fun digest(): Digest = this.hashCode().toLong()
}