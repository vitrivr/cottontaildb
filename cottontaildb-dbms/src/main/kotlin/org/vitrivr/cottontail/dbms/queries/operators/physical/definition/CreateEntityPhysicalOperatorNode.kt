package org.vitrivr.cottontail.dbms.queries.operators.physical.definition

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.dbms.column.ColumnMetadata
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.definition.CreateEntityOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext

/**
 * A [DataDefinitionPhysicalOperatorNode] used to create new [Entity].
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class CreateEntityPhysicalOperatorNode(override val context: QueryContext, val entity: Name.EntityName, val cols: Map<Name.ColumnName, ColumnMetadata>, val mayExist: Boolean) : DataDefinitionPhysicalOperatorNode("CreateEntity") {
    override fun copy(): CreateEntityPhysicalOperatorNode = CreateEntityPhysicalOperatorNode(this.context, this.entity, this.cols, this.mayExist)
    override fun toOperator(ctx: QueryContext): Operator = CreateEntityOperator(this.entity, this.cols, this.mayExist, ctx)
    override fun digest(): Digest = this.hashCode().toLong()
}