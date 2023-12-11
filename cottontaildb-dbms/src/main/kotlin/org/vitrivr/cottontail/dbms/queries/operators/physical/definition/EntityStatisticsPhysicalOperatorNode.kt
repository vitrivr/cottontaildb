package org.vitrivr.cottontail.dbms.queries.operators.physical.definition

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.operators.definition.EntityStatisticsOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.ColumnSets

/**
 * A [DataDefinitionPhysicalOperatorNode] used to query statistics of an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class EntityStatisticsPhysicalOperatorNode(
    val tx: CatalogueTx,
    val entityName: Name.EntityName,
    context: QueryContext
): DataDefinitionPhysicalOperatorNode("EntityStatistics", context, ColumnSets.DDL_INTROSPECTION_COLUMNS) {
    override fun copy() = EntityStatisticsPhysicalOperatorNode(this.tx, this.entityName, this.context)
    override fun toOperator(ctx: QueryContext) = EntityStatisticsOperator(this.tx, this.entityName, ctx)
    override fun digest(): Digest = this.hashCode().toLong()
}