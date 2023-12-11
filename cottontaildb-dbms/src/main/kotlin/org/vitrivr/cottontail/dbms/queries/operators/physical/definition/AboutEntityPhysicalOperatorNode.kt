package org.vitrivr.cottontail.dbms.queries.operators.physical.definition

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.operators.definition.AboutEntityOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.ColumnSets.DDL_ABOUT_COLUMNS

/**
 * A [DataDefinitionPhysicalOperatorNode] used to query information about an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class AboutEntityPhysicalOperatorNode(
    val tx: CatalogueTx,
    val entityName: Name.EntityName,
    context: QueryContext
): DataDefinitionPhysicalOperatorNode("AboutEntity", context, DDL_ABOUT_COLUMNS) {
    override fun copy() = AboutEntityPhysicalOperatorNode(this.tx, this.entityName, this.context)
    override fun toOperator(ctx: QueryContext) = AboutEntityOperator(this.tx, this.entityName, ctx)
    override fun digest(): Digest = this.hashCode().toLong()
}