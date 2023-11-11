package org.vitrivr.cottontail.dbms.queries.operators.physical.definition

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.dbms.execution.operators.definition.DropSchemaOperator
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * A [DataDefinitionPhysicalOperatorNode] used to drop an [Index].
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class DropSchemaPhysicalOperatorNode(override val context: QueryContext, val schema: Name.SchemaName): DataDefinitionPhysicalOperatorNode("DropSchema") {
    override fun copy() = DropSchemaPhysicalOperatorNode(this.context, this.schema)
    override fun toOperator(ctx: QueryContext) = DropSchemaOperator(this.schema, ctx)
    override fun digest(): Digest = this.hashCode().toLong()
}