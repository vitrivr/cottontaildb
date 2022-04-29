package org.vitrivr.cottontail.dbms.queries.operators.physical.definition

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.execution.operators.definition.DropSchemaOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.schema.Schema

/**
 * A [DataDefinitionPhysicalOperatorNode] used to drop a [Schema].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class DropSchemaPhysicalOperatorNode(val tx: CatalogueTx, val schemaName: Name.SchemaName): DataDefinitionPhysicalOperatorNode("DropSchema") {
    override fun copy() = DropSchemaPhysicalOperatorNode(this.tx, this.schemaName)
    override fun toOperator(ctx: QueryContext) = DropSchemaOperator(this.tx, this.schemaName)
}