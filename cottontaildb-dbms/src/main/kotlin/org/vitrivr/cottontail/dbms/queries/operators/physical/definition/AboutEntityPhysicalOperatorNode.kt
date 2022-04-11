package org.vitrivr.cottontail.dbms.queries.operators.physical.definition

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.operators.definition.AboutEntityOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.ColumnSets.DDL_ABOUT_COLUMNS

/**
 * A [DataDefinitionPhysicalOperatorNode] used to query information about an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class AboutEntityPhysicalOperatorNode(val tx: CatalogueTx, val entityName: Name.EntityName): DataDefinitionPhysicalOperatorNode("AboutEntity") {
    override val columns: List<ColumnDef<*>> = DDL_ABOUT_COLUMNS
    override fun copy() = AboutEntityPhysicalOperatorNode(this.tx, this.entityName)
    override fun toOperator(ctx: QueryContext) = AboutEntityOperator(this.tx, this.entityName)
}