package org.vitrivr.cottontail.dbms.queries.operators.physical.definition

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.definition.ListEntityOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.ColumnSets
import org.vitrivr.cottontail.dbms.queries.operators.basics.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.schema.Schema

/**
 * A [DataDefinitionPhysicalOperatorNode] used to list all [Entity] entries in the [Catalogue] (optionally, for a given [Schema])
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class ListEntityPhysicalOperatorNode(override val context: QueryContext, val schema: Name.SchemaName? = null): DataDefinitionPhysicalOperatorNode("ListEntity") {
    override val columns: List<ColumnDef<*>> = ColumnSets.DDL_LIST_COLUMNS
    override fun copy(): NullaryPhysicalOperatorNode = ListEntityPhysicalOperatorNode(this.context, this.schema)
    override fun toOperator(ctx: QueryContext): Operator = ListEntityOperator(this.schema, ctx)
    override fun digest(): Digest = this.hashCode().toLong()
}