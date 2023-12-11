package org.vitrivr.cottontail.dbms.queries.operators.physical.management

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.nodes.traits.NotPartitionableTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.management.InsertOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.NullaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.management.InsertLogicalOperatorNode
import org.vitrivr.cottontail.dbms.statistics.values.ValueStatistics

/**
 * A [InsertPhysicalOperatorNode] that formalizes a INSERT operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 2.9.0
 */
@Suppress("UNCHECKED_CAST")
class InsertPhysicalOperatorNode(override val groupId: GroupId, val context: QueryContext, val entity: EntityTx, val tuples: MutableList<Tuple>) : NullaryPhysicalOperatorNode() {
    companion object {
        private const val NODE_NAME = "Insert"
    }

    /** The name of this [InsertPhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [InsertLogicalOperatorNode] produces the columns defined in the [InsertOperator] */
    override val columns: List<Binding.Column> = InsertOperator.COLUMNS.map {
        this.context.bindings.bind(it, null)
    }

    /** The statistics for this [InsertPhysicalOperatorNode]. */
    override val statistics = Object2ObjectLinkedOpenHashMap<ColumnDef<*>, ValueStatistics<*>>()

    /** The [InsertPhysicalOperatorNode] produces a single record. */
    override val outputSize: Long = 1L

    /** The [Cost] incurred by this [InsertPhysicalOperatorNode]. */
    override val cost: Cost

    /** The [InsertPhysicalOperatorNode] cannot be partitioned. */
    override val traits: Map<TraitType<*>, Trait> = mapOf(NotPartitionableTrait to NotPartitionableTrait)

    init {
        /* Obtain statistics costs and  */
        var estimatedInsertSize = 0
        this.entity.listColumns().forEach { columnDef ->
            val statistic = this.entity.columnForName(columnDef.name).newTx(this.entity.context).statistics() as ValueStatistics<Value>
            this.statistics[columnDef] = statistic
            estimatedInsertSize += if (columnDef.type == Types.String) {
                statistic.avgWidth * Char.SIZE_BYTES  /* GA: This is not a good cost estimate for empty tables but we don't really need a better one. */
            } else {
                columnDef.type.physicalSize
            }
        }

        this.cost = (Cost.DISK_ACCESS_WRITE + Cost.MEMORY_ACCESS) * estimatedInsertSize * this.tuples.size
    }

    /**
     * Creates and returns a copy of this [InsertLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [InsertLogicalOperatorNode].
     */
    override fun copy() = InsertPhysicalOperatorNode(this.groupId, this.context, this.entity, this.tuples)

    /**
     * An [InsertPhysicalOperatorNode] is always executable
     *
     * @param ctx The [QueryContext] to check.
     * @return True
     */
    override fun canBeExecuted(ctx: QueryContext): Boolean = true

    /**
     * Converts this [InsertPhysicalOperatorNode] to a [InsertOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator = InsertOperator(this.groupId, this.entity, this.tuples, ctx)

    /**
     * The [InsertPhysicalOperatorNode] cannot be partitioned.
     */
    override fun tryPartition(ctx: QueryContext, max: Int): Physical? = null

    /**
     * Generates and returns a [Digest] for this [InsertPhysicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest {
        var result = this.entity.dbo.name.hashCode().toLong()
        result += 31L * result + this.tuples.hashCode()
        return result
    }
}