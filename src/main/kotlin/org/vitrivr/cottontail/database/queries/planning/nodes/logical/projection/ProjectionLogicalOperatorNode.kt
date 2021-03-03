package org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection.ProjectionPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.projection.Projection
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.exceptions.QueryException

/**
 * A [UnaryLogicalOperatorNode] that represents a projection operation on a [org.vitrivr.cottontail.model.recordset.Recordset].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class ProjectionLogicalOperatorNode(input: OperatorNode.Logical, val type: Projection = Projection.SELECT, val fields: List<Pair<Name.ColumnName, Name.ColumnName?>>) : UnaryLogicalOperatorNode(input) {

    init {
        /* Sanity check. */
        if (this.fields.isEmpty()) {
            throw QueryException.QuerySyntaxException("Projection of type $type must specify at least one column.")
        }
    }

    /** The [ProjectionLogicalOperatorNode] maps the [ColumnDef] of its input to the output specified by [fields]. */
    override val columns: Array<ColumnDef<*>>
        get() {
            val input = this.input
            return when (type) {
                Projection.SELECT,
                Projection.SELECT_DISTINCT -> {
                    return this.fields.map { f ->
                        val column = input.columns.find { c -> f.first.matches(c.name) }
                            ?: throw QueryException.QueryBindException("Column with name $f could not be found on input.")
                        column.copy(name = f.second ?: column.name)
                    }.toTypedArray()
                }
                Projection.EXISTS -> {
                    val alias = fields.first().second
                    val name = alias ?: (input.columns.first().name.entity()?.column(Projection.COUNT.label()) ?: Name.ColumnName(Projection.COUNT.label()))
                    arrayOf(ColumnDef(name, Type.Boolean, false))
                }
                Projection.COUNT,
                Projection.COUNT_DISTINCT -> {
                    val alias = fields.first().second
                    val name = alias ?: (input.columns.first().name.entity()?.column(Projection.COUNT.label()) ?: Name.ColumnName(Projection.COUNT.label()))
                    arrayOf(ColumnDef(name, Type.Long,  false))
                }
                Projection.MAX,
                Projection.MIN,
                Projection.SUM,
                Projection.MEAN -> {
                    return this.fields.map { f ->
                        val column = input.columns.find { c -> c.name == f.first }
                            ?: throw QueryException.QueryBindException("Column with name $f could not be found on input.")
                        if (!column.type.numeric) throw QueryException.QueryBindException("Projection of type $type can only be applied to numeric column, which $column isn't.")
                        column.copy(name = f.second ?: column.name)
                    }.toTypedArray()
                }
            }
        }

    /**
     * Returns a copy of this [ProjectionLogicalOperatorNode] and its input.
     *
     * @return Copy of this [ProjectionLogicalOperatorNode] and its input.
     */
    override fun copyWithInputs(): ProjectionLogicalOperatorNode = ProjectionLogicalOperatorNode(this.input.copyWithInputs(), this.type, this.fields)

    /**
     * Returns a copy of this [ProjectionLogicalOperatorNode] and its output.
     *
     * @param input The [OperatorNode.Logical] that should act as inputs.
     * @return Copy of this [ProjectionLogicalOperatorNode] and its output.
     */
    override fun copyWithOutput(input: OperatorNode.Logical?): OperatorNode.Logical {
        require(input != null) { "Input is required for unary logical operator node." }
        val projection = ProjectionLogicalOperatorNode(input, this.type, this.fields)
        return (this.output?.copyWithOutput(projection) ?: projection)
    }

    /**
     * Returns a [ProjectionPhysicalOperatorNode] representation of this [ProjectionLogicalOperatorNode]
     *
     * @return [ProjectionPhysicalOperatorNode]
     */
    override fun implement(): Physical = ProjectionPhysicalOperatorNode(this.input.implement(), this.type, this.fields)

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is ProjectionLogicalOperatorNode) return false

        if (type != other.type) return false
        if (fields != other.fields) return false

        return true
    }

    override fun hashCode(): Int {
        var result = type.hashCode()
        result = 31 * result + fields.hashCode()
        return result
    }
}