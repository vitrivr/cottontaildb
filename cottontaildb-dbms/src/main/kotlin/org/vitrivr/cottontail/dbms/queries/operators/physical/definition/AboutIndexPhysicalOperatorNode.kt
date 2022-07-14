package org.vitrivr.cottontail.dbms.queries.operators.physical.definition

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.execution.operators.definition.AboutIndexOperator
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.ColumnSets

/**
 * A [DataDefinitionPhysicalOperatorNode] used to query information about an [Index].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class AboutIndexPhysicalOperatorNode (val tx: CatalogueTx, val indexName: Name.IndexName): DataDefinitionPhysicalOperatorNode("AboutIndex") {
    override val columns: List<ColumnDef<*>> = ColumnSets.DDL_ABOUT_COLUMNS
    override fun copy() = AboutIndexPhysicalOperatorNode(this.tx, this.indexName)
    override fun toOperator(ctx: QueryContext) = AboutIndexOperator(this.tx, this.indexName)
}