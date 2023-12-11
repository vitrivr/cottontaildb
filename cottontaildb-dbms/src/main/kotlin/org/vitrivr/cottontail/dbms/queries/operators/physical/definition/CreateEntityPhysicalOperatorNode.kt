package org.vitrivr.cottontail.dbms.queries.operators.physical.definition

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.column.ColumnMetadata
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.definition.CreateEntityOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.ColumnSets

/**
 * A [DataDefinitionPhysicalOperatorNode] used to create new [Entity].
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class CreateEntityPhysicalOperatorNode(
   private val tx: CatalogueTx,
   val entityName: Name.EntityName,
   val mayExist: Boolean,
   val createColumns: List<Pair<Name.ColumnName, ColumnMetadata>>,
   context: QueryContext
) : DataDefinitionPhysicalOperatorNode("CreateEntity", context, ColumnSets.DDL_STATUS_COLUMNS) {
    override fun copy(): CreateEntityPhysicalOperatorNode = CreateEntityPhysicalOperatorNode(this.tx, this.entityName, this.mayExist, this.createColumns, this.context)
    override fun toOperator(ctx: QueryContext): Operator = CreateEntityOperator(this.tx, this.entityName, this.createColumns,  this.mayExist, ctx)
    override fun digest(): Digest = this.hashCode().toLong()
}