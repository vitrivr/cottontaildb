package org.vitrivr.cottontail.database.queries.planning.nodes.logical

import org.vitrivr.cottontail.database.queries.components.Projection
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.exceptions.QueryException

/**
 * A [NodeExpression.LogicalNodeExpression] that represents a projection operation on a [org.vitrivr.cottontail.model.recordset.Recordset].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class ProjectionLogicalNodeExpression(val type: Projection = Projection.SELECT, val fields: List<Pair<ColumnDef<*>, Name.ColumnName?>>) : NodeExpression.LogicalNodeExpression() {
    init {
        /* Sanity check. */
        when (type) {
            Projection.SELECT -> if (this.fields.isEmpty()) {
                throw QueryException.QuerySyntaxException("Projection of type $type must specify at least one column.")
            }
            Projection.SELECT_DISTINCT -> if (this.fields.isEmpty()) {
                throw QueryException.QuerySyntaxException("Projection of type $type must specify at least one column.")
            }
            Projection.COUNT_DISTINCT -> if (this.fields.isEmpty()) {
                throw QueryException.QuerySyntaxException("Projection of type $type must specify at least one column.")
            }
            Projection.MAX -> if (this.fields.isEmpty()) {
                throw QueryException.QuerySyntaxException("Projection of type $type must specify a column.")
            } else if (!this.fields.first().first.type.numeric) {
                throw QueryException.QueryBindException("Projection of type $type can only be applied on a numeric column, which ${fields.first().first.name} is not.")
            }
            Projection.MIN -> if (this.fields.isEmpty()) {
                throw QueryException.QuerySyntaxException("Projection of type $type must specify a column.")
            } else if (!this.fields.first().first.type.numeric) {
                throw QueryException.QueryBindException("Projection of type $type can only be applied to a numeric column, which ${fields.first().first.name} is not.")
            }
            Projection.SUM -> if (this.fields.isEmpty()) {
                throw QueryException.QuerySyntaxException("Projection of type $type must specify a column.")
            } else if (!this.fields.first().first.type.numeric) {
                throw QueryException.QueryBindException("Projection of type $type can only be applied to a numeric column, which ${fields.first().first.name} is not.")
            }
            Projection.MEAN -> if (fields.isEmpty()) {
                throw QueryException.QuerySyntaxException("Projection of type $type must specify a column.")
            } else if (!this.fields.first().first.type.numeric) {
                throw QueryException.QueryBindException("Projection of type $type can only be applied to a numeric column, which ${fields.first().first.name} is not.")
            }
            else -> {
            }
        }
    }

    /** Input arity of [ProjectionLogicalNodeExpression] is always one, since it acts on a single [org.vitrivr.cottontail.model.recordset.Recordset]. */
    override val inputArity: Int = 1

    /**
     * Returns a copy of this [ProjectionLogicalNodeExpression]
     *
     * @return Copy of this [ProjectionLogicalNodeExpression]
     */
    override fun copy(): ProjectionLogicalNodeExpression = ProjectionLogicalNodeExpression(this.type, this.fields)
}