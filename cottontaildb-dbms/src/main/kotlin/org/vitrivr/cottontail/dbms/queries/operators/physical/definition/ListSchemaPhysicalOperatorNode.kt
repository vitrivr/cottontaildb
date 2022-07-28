package org.vitrivr.cottontail.dbms.queries.operators.physical.definition

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.definition.ListSchemaOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.ColumnSets
import org.vitrivr.cottontail.dbms.queries.operators.basics.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.schema.Schema

/**
 * A [DataDefinitionPhysicalOperatorNode] used list all [Schema] entries in the [Catalogue]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class ListSchemaPhysicalOperatorNode(val tx: CatalogueTx): DataDefinitionPhysicalOperatorNode("ListSchema") {
    override val columns: List<ColumnDef<*>> = ColumnSets.DDL_LIST_COLUMNS
    override fun copy(): NullaryPhysicalOperatorNode = ListSchemaPhysicalOperatorNode(this.tx)
    override fun toOperator(ctx: QueryContext): Operator = ListSchemaOperator(this.tx, ctx)
}