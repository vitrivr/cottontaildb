package org.vitrivr.cottontail.dbms.queries.operators.physical.definition

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.definition.CreateSchemaOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.NullaryPhysicalOperatorNode

/**
 * A [DataDefinitionPhysicalOperatorNode] used to create new entities.
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class CreateSchemaPhysicalOperatorNode(override val context: QueryContext, val schema: Name.SchemaName, val mayExist: Boolean): DataDefinitionPhysicalOperatorNode("CreateSchema") {
    override fun copy(): NullaryPhysicalOperatorNode = CreateSchemaPhysicalOperatorNode(this.context, this.schema, this.mayExist)
    override fun toOperator(ctx: QueryContext): Operator = CreateSchemaOperator(this.schema, this.mayExist, ctx)
    override fun digest(): Digest = this.hashCode().toLong()
}