package org.vitrivr.cottontail.database.queries.planning.nodes.physical.management

import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.queries.OperatorNode
import org.vitrivr.cottontail.database.queries.QueryContext
import org.vitrivr.cottontail.database.queries.binding.RecordBinding
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.planning.nodes.physical.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.database.statistics.entity.RecordStatistics
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.execution.operators.basics.Operator
import org.vitrivr.cottontail.execution.operators.management.InsertOperator
import org.vitrivr.cottontail.execution.operators.management.UpdateOperator

/**
 * A [InsertPhysicalOperatorNode] that formalizes a INSERT operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class InsertPhysicalOperatorNode(val entity: Entity, val records: MutableList<RecordBinding>) : NullaryPhysicalOperatorNode() {

    /** The [InsertPhysicalOperatorNode] produces the [ColumnDef]s defined in the [UpdateOperator]. */
    override val columns: Array<ColumnDef<*>> = InsertOperator.COLUMNS

    /** The [RecordStatistics] for this [InsertPhysicalOperatorNode]. */
    override val statistics: RecordStatistics = this.entity.statistics

    /** The [InsertPhysicalOperatorNode] produces a single record. */
    override val outputSize: Long = 1L

    /** The [Cost] of this [InsertPhysicalOperatorNode]. */
    override val cost: Cost = Cost(this.records.size * this.records.first().size * Cost.COST_DISK_ACCESS_WRITE)

    /** The [InsertPhysicalOperatorNode] cannot be partitioned. */
    override val canBePartitioned: Boolean = false

    /** The [InsertPhysicalOperatorNode] is always executable. */
    override val executable: Boolean = true

    /**
     * Returns a copy of this [InsertPhysicalOperatorNode] and its input.
     *
     * @return Copy of this [InsertPhysicalOperatorNode] and its input.
     */
    override fun copyWithInputs(): InsertPhysicalOperatorNode = InsertPhysicalOperatorNode(this.entity, this.records)

    /**
     * Returns a copy of this [InsertPhysicalOperatorNode] and its output.
     *
     * @param inputs The [OperatorNode] that should act as inputs.
     * @return Copy of this [InsertPhysicalOperatorNode] and its output.
     */
    override fun copyWithOutput(vararg inputs: OperatorNode.Physical): OperatorNode.Physical {
        require(inputs.size == 0) { "No input is allowed for nullary operators." }
        val insert = InsertPhysicalOperatorNode(this.entity, this.records)
        return (this.output?.copyWithOutput(insert) ?: insert)
    }

    /**
     * Converts this [InsertPhysicalOperatorNode] to a [InsertOperator].
     *
     * @param tx The [TransactionContext] used for execution.
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(tx: TransactionContext, ctx: QueryContext): Operator = InsertOperator(this.entity, this.records.map { it.bind(ctx) })

    /**
     * [InsertPhysicalOperatorNode] cannot be partitioned.
     */
    override fun partition(p: Int): List<Physical> {
        throw UnsupportedOperationException("InsertPhysicalOperatorNode cannot be partitioned.")
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is InsertPhysicalOperatorNode) return false

        if (entity != other.entity) return false
        if (records != other.records) return false

        return true
    }

    override fun hashCode(): Int {
        var result = entity.hashCode()
        result = 31 * result + records.hashCode()
        return result
    }
}