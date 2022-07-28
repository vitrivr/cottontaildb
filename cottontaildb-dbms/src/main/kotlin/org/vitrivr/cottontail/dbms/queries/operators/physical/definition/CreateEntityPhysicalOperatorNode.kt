package org.vitrivr.cottontail.dbms.queries.operators.physical.definition

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.definition.CreateEntityOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * A [DataDefinitionPhysicalOperatorNode] used to create new [Entity].
 *
 * @author Ralph Gasser
 * @version 1.0.0.
 */
class CreateEntityPhysicalOperatorNode(private val tx: CatalogueTx, private val entityName: Name.EntityName, private val cols: Array<ColumnDef<*>>) : DataDefinitionPhysicalOperatorNode("CreateEntity") {
    override fun copy(): CreateEntityPhysicalOperatorNode = CreateEntityPhysicalOperatorNode(this.tx, this.entityName, this.cols)
    override fun toOperator(ctx: QueryContext): Operator = CreateEntityOperator(this.tx, this.entityName, this.cols, ctx)
}