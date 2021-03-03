package org.vitrivr.cottontail.database.queries.planning.nodes.physical.projection

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.logical.projection.ProjectionLogicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.predicates.FilterPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.projection.Projection
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.projection.*
import org.vitrivr.cottontail.execution.operators.transform.LimitOperator
import org.vitrivr.cottontail.model.basics.Name
import org.vitrivr.cottontail.model.basics.Type
import org.vitrivr.cottontail.model.exceptions.QueryException

/**
 * Formalizes a [UnaryPhysicalOperatorNode] operation in the Cottontail DB query execution engine.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class ProjectionPhysicalOperatorNode(input: OperatorNode.Physical, val type: Projection, val fields: List<Pair<Name.ColumnName, Name.ColumnName?>>) : UnaryPhysicalOperatorNode(input) {
    init {
        /* Sanity check. */
        if (this.fields.isEmpty()) {
            throw QueryException.QuerySyntaxException("Projection of type $type must specify at least one column.")
        }
    }

    /** The [ProjectionLogicalOperatorNode] maps the [ColumnDef] of its input to the output specified by [fields]. */
    override val columns: Array<ColumnDef<*>>
        get() {
            return when (type) {
                Projection.SELECT,
                Projection.SELECT_DISTINCT -> {
                    this.input.columns.mapNotNull { c ->
                        val find = this.fields.find { f -> f.first.matches(c.name) }
                        if (find != null) {
                            c
                        } else {
                            null
                        }
                    }.toTypedArray()
                }
                Projection.EXISTS -> {
                    val alias = fields.first().second
                    val name = alias ?:
                    (this.input.columns.first().name.entity()
                        ?.column(Projection.COUNT.label())
                        ?: Name.ColumnName(Projection.COUNT.label()))
                    arrayOf(ColumnDef(name, Type.Boolean, false))
                }
                Projection.COUNT,
                Projection.COUNT_DISTINCT -> {
                    val alias = fields.first().second
                    val name = alias ?:
                    (this.input.columns.first().name.entity()
                        ?.column(Projection.COUNT.label())
                        ?: Name.ColumnName(Projection.COUNT.label()))
                    arrayOf(ColumnDef(name, Type.Long, false))
                }
                Projection.MAX,
                Projection.MIN,
                Projection.SUM,
                Projection.MEAN -> {
                    return this.fields.map { f ->
                        val column = this.input.columns.find { c -> c.name == f.first }
                            ?: throw QueryException.QueryBindException("Column with name $f could not be found on input.")
                        if (!column.type.numeric) throw QueryException.QueryBindException("Projection of type $type can only be applied to numeric column, which $column isn't.")
                        column.copy(name = f.second ?: column.name)
                    }.toTypedArray()
                }
            }
        }

    /** The output size of this [ProjectionPhysicalOperatorNode], which equals its input's output size. */
    override val outputSize: Long
        get() = this.input.outputSize

    /** The [Cost] of a [ProjectionPhysicalOperatorNode]. */
    override val cost: Cost = Cost(cpu = this.outputSize * this.fields.size * Cost.COST_MEMORY_ACCESS)

    /**
     * Returns a copy of this [ProjectionPhysicalOperatorNode] and its input.
     *
     * @return Copy of this [ProjectionPhysicalOperatorNode] and its input.
     */
    override fun copyWithInputs() = ProjectionPhysicalOperatorNode(this.input.copyWithInputs(), this.type, this.fields)

    /**
     * Returns a copy of this [ProjectionPhysicalOperatorNode] and its output.
     *
     * @param input The [OperatorNode] that should act as inputs.
     * @return Copy of this [ProjectionPhysicalOperatorNode] and its output.
     */
    override fun copyWithOutput(input: OperatorNode.Physical?): OperatorNode.Physical {
        require(input != null) { "Input is required for copyWithOutput() on unary physical operator node." }
        val projection = ProjectionPhysicalOperatorNode(input, this.type, this.fields)
        return (this.output?.copyWithOutput(projection) ?: projection)
    }

    /**
     * Partitions this [ProjectionPhysicalOperatorNode].
     *
     * @param p The number of partitions to create.
     * @return List of [OperatorNode.Physical], each representing a partition of the original tree.
     */
    override fun partition(p: Int): List<Physical> = this.input.partition(p).map { ProjectionPhysicalOperatorNode(it, this.type, this.fields) }

    /**
     * Converts this [ProjectionPhysicalOperatorNode] to a [LimitOperator].
     *
     * @param tx The [TransactionContext] used for execution.
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(tx: TransactionContext, ctx: QueryContext) = when (this.type) {
        Projection.SELECT -> SelectProjectionOperator(this.input.toOperator(tx, ctx), this.fields)
        Projection.SELECT_DISTINCT -> TODO()
        Projection.COUNT -> CountProjectionOperator(this.input.toOperator(tx, ctx))
        Projection.COUNT_DISTINCT -> TODO()
        Projection.EXISTS -> ExistsProjectionOperator(this.input.toOperator(tx, ctx))
        Projection.SUM -> SumProjectionOperator(this.input.toOperator(tx, ctx), this.fields)
        Projection.MAX -> MaxProjectionOperator(this.input.toOperator(tx, ctx), this.fields)
        Projection.MIN -> MinProjectionOperator(this.input.toOperator(tx, ctx), this.fields)
        Projection.MEAN -> MeanProjectionOperator(this.input.toOperator(tx, ctx), this.fields)
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is ProjectionPhysicalOperatorNode) return false

        if (type != other.type) return false
        if (fields != other.fields) return false

        return true
    }

    override fun hashCode(): Int {
        var result = type.hashCode()
        result = 31 * result + fields.hashCode()
        return result
    }

    /** Generates and returns a [String] representation of this [FilterPhysicalOperatorNode]. */
    override fun toString() = "${super.toString()}(${this.columns.joinToString(",") { it.name.toString() }})"
}