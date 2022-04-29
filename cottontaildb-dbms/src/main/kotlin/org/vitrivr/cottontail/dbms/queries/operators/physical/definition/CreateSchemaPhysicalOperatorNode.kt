package org.vitrivr.cottontail.dbms.queries.operators.physical.definition

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.definition.CreateSchemaOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.physical.NullaryPhysicalOperatorNode

/**
 * A [DataDefinitionPhysicalOperatorNode] used to create new entities.
 *
 * @author Ralph Gasser
 * @version 1.0.0.
 */
class CreateSchemaPhysicalOperatorNode(val tx: CatalogueTx, val schemaName: Name.SchemaName): DataDefinitionPhysicalOperatorNode("CreateSchema") {
    override fun copy(): NullaryPhysicalOperatorNode = CreateSchemaPhysicalOperatorNode(this.tx, this.schemaName)
    override fun toOperator(ctx: QueryContext): Operator = CreateSchemaOperator(this.tx, this.schemaName)
}