package org.vitrivr.cottontail.dbms.queries.operators.physical.management

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.column.ColumnTx
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.management.InsertOperator
import org.vitrivr.cottontail.dbms.execution.operators.management.UpdateOperator
import org.vitrivr.cottontail.dbms.queries.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.logical.management.InsertLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.statistics.columns.ValueStatistics

/**
 * A [InsertPhysicalOperatorNode] that formalizes a INSERT operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 2.5.0
 */
class InsertPhysicalOperatorNode(override val groupId: GroupId, val entity: EntityTx, val records: MutableList<Record>) : NullaryPhysicalOperatorNode() {
    companion object {
        private const val NODE_NAME = "Insert"
    }

    /** The name of this [InsertPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The physical [ColumnDef] accessed by the [InsertPhysicalOperatorNode]. */
    override val physicalColumns: List<ColumnDef<*>> = this.entity.listColumns()

    /** The [InsertPhysicalOperatorNode] produces the [ColumnDef]s defined in the [UpdateOperator]. */
    override val columns: List<ColumnDef<*>> = InsertOperator.COLUMNS

    /** The statistics for this [InsertPhysicalOperatorNode]. */
    override val statistics = Object2ObjectLinkedOpenHashMap<ColumnDef<*>, ValueStatistics<*>>()

    /** The [InsertPhysicalOperatorNode] produces a single record. */
    override val outputSize: Long = 1L

    /** The [Cost] of this [InsertPhysicalOperatorNode]. */
    override val cost: Cost

    /** The [InsertPhysicalOperatorNode] cannot be partitioned. */
    override val canBePartitioned: Boolean = false

    /** The [InsertPhysicalOperatorNode] is always executable. */
    override val executable: Boolean = true

    init {
        /* Obtain statistics costs. */
        this.entity.listColumns().forEach { columnDef ->
            this.statistics[columnDef] = (this.entity.context.getTx(this.entity.columnForName(columnDef.name)) as ColumnTx<*>).statistics() as ValueStatistics<Value>
        }
        val cummulativeAvgRecordSize = this.records.sumOf { r -> r.columns.sumOf { c -> r[c]!!.logicalSize }}
        this.cost = (Cost.DISK_ACCESS_WRITE + Cost.MEMORY_ACCESS) * cummulativeAvgRecordSize
    }

    /**
     * Creates and returns a copy of this [InsertLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [InsertLogicalOperatorNode].
     */
    override fun copy() = InsertPhysicalOperatorNode(this.groupId, this.entity, this.records)

    /**
     * Bind calls have no effect on [InsertPhysicalOperatorNode]
     *
     * @param context The new [BindingContext]
     */
    override fun bind(context: BindingContext) { /* No op. */ }

    /**
     * Converts this [InsertPhysicalOperatorNode] to a [InsertOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator = InsertOperator(this.groupId, this.entity, this.records)

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