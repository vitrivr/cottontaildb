package org.vitrivr.cottontail.dbms.queries.operators.physical.definition

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.operators.definition.EntityStatisticsOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.ColumnSets

/**
 * A [DataDefinitionPhysicalOperatorNode] used to query statistics of an [Entity].
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class EntityStatisticsPhysicalOperatorNode(override val context: QueryContext, val entity: Name.EntityName): DataDefinitionPhysicalOperatorNode("EntityStatistics") {
    override val columns: List<ColumnDef<*>> = ColumnSets.DDL_INTROSPECTION_COLUMNS
    override fun copy() = EntityStatisticsPhysicalOperatorNode(this.context, this.entity)
    override fun toOperator(ctx: QueryContext) = EntityStatisticsOperator(this.entity, ctx)
    override fun digest(): Digest = this.hashCode().toLong()
}