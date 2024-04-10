package org.vitrivr.cottontail.dbms.queries.operators.physical.definition

import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.operators.definition.AnalyseEntityOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.ColumnSets

/**
 * A [DataDefinitionPhysicalOperatorNode] used to analyze an [Entity].
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
class AnalyseEntityPhysicalOperatorNode(
    val tx: CatalogueTx,
    val entityName: Name.EntityName,
    context: QueryContext
): DataDefinitionPhysicalOperatorNode("AnalyseEntity", context, ColumnSets.DDL_STATUS_COLUMNS) {
    override fun copy() = AnalyseEntityPhysicalOperatorNode(this.tx, this.entityName, this.context)
    override fun toOperator(ctx: QueryContext) = AnalyseEntityOperator(this.tx, this.entityName, ctx)
    override fun digest(): Digest = this.hashCode().toLong()
}

