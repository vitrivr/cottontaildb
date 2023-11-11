package org.vitrivr.cottontail.dbms.queries.operators.physical.definition

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.operators.definition.AboutEntityOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.ColumnSets.DDL_ABOUT_COLUMNS

/**
 * A [DataDefinitionPhysicalOperatorNode] used to query information about an [Entity].
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class AboutEntityPhysicalOperatorNode(override val context: QueryContext, val entity: Name.EntityName): DataDefinitionPhysicalOperatorNode("AboutEntity") {
    override val columns: List<ColumnDef<*>> = DDL_ABOUT_COLUMNS
    override fun copy() = AboutEntityPhysicalOperatorNode(this.context, this.entity)
    override fun toOperator(ctx: QueryContext) = AboutEntityOperator(this.entity, ctx)
    override fun digest(): Digest = this.hashCode().toLong()
}