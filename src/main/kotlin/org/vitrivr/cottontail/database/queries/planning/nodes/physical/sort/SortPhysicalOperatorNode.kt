package org.vitrivr.cottontail.database.queries.planning.nodes.physical.sort

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.queries.sort.SortOrder
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.sort.HeapSortOperator
import org.vitrivr.cottontail.model.exceptions.QueryException

/**
 * A [UnaryPhysicalOperatorNode] that represents sorting the input by a set of specified [ColumnDef]s. Internally,
 * a heap sort algorithm is applied for sorting.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class SortPhysicalOperatorNode(sortOn: Array<Pair<ColumnDef<*>, SortOrder>>) : UnaryPhysicalOperatorNode() {
    init {
        if (sortOn.isEmpty()) throw QueryException.QuerySyntaxException("At least one column must be specified for sorting.")
    }

    /** The [SortPhysicalOperatorNode] returns the [ColumnDef] of its input. */
    override val columns: Array<ColumnDef<*>>
        get() = this.input.columns

    /** The [SortPhysicalOperatorNode] requires all [ColumnDef]s used on the ORDER BY clause. */
    override val requires: Array<ColumnDef<*>> = sortOn.map { it.first }.toTypedArray()

    /** The number of [Record]s produced by this [SortPhysicalOperatorNode]. */
    override val outputSize: Long
        get() = this.input.outputSize

    /** The [Cost] incurred by this [SortPhysicalOperatorNode]. */
    override val cost: Cost
        get() = Cost(
            cpu = 2 * this.input.outputSize * this.order.size * Cost.COST_MEMORY_ACCESS,
            memory = (this.columns.map { it.type.physicalSize }.sum() * this.outputSize).toFloat()
        )

    /** A [SortPhysicalOperatorNode] orders the input in by the specified [ColumnDef]s. */
    override val order = sortOn

    /** Copies this [SortPhysicalOperatorNode]. */
    override fun copy(): SortPhysicalOperatorNode = SortPhysicalOperatorNode(this.order)

    /** Converts this [SortPhysicalOperatorNode] to a [HeapSortOperator]. */
    override fun toOperator(tx: TransactionContext, ctx: QueryContext): Operator = HeapSortOperator(
        this.input.toOperator(tx, ctx),
        this.order,
        if (this.outputSize > Integer.MAX_VALUE) {
            Integer.MAX_VALUE
            /** TODO: This case requires special handling. */
        } else {
            this.outputSize.toInt()
        }
    )
}