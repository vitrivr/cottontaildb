package org.vitrivr.cottontail.dbms.queries.operators.physical.management

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.nodes.traits.NotPartitionableTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.management.UpdateOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.physical.UnaryPhysicalOperatorNode

/**
 * A [UpdatePhysicalOperatorNode] that formalizes a UPDATE operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 2.2.1
 */
class UpdatePhysicalOperatorNode(input: Physical? = null, val entity: EntityTx, val values: List<Pair<ColumnDef<*>, Binding>>) : UnaryPhysicalOperatorNode(input) {
    companion object {
        private const val NODE_NAME = "Update"
    }

    /** The name of this [UpdatePhysicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [UpdatePhysicalOperatorNode] produces the [ColumnDef]s defined in the [UpdateOperator]. */
    override val columns: List<ColumnDef<*>> = UpdateOperator.COLUMNS

    /** The [UpdatePhysicalOperatorNode] requires the [ColumnDef] that are being updated. */
    override val requires: List<ColumnDef<*>> = this.values.map { it.first }

    /** The [UpdatePhysicalOperatorNode] produces a single record. */
    override val outputSize: Long = 1L

    /** The [Cost] of this [UpdatePhysicalOperatorNode]. */
    override val cost: Cost
        get() = ((Cost.DISK_ACCESS_WRITE + Cost.MEMORY_ACCESS) * (this.input?.columns?.sumOf { this.statistics[it]!!.avgWidth } ?: 0 )) * (this.input?.outputSize ?: 0)

    /** The [UpdatePhysicalOperatorNode] cannot be partitioned. */
    override val traits: Map<TraitType<*>, Trait> = mapOf(NotPartitionableTrait to NotPartitionableTrait)


    /**
     * Creates and returns a copy of this [UpdatePhysicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [UpdatePhysicalOperatorNode].
     */
    override fun copy() = UpdatePhysicalOperatorNode(entity = this.entity, values = this.values)

    /**
     * Converts this [UpdatePhysicalOperatorNode] to a [UpdateOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator {
        val entries = this.values.map { it.first to it.second.value }
        return UpdateOperator(
            this.input?.toOperator(ctx) ?: throw IllegalStateException("Cannot convert disconnected OperatorNode to Operator (node = $this)"),
            this.entity,
            entries
        )
    }

    override fun toString(): String = "${super.toString()}[${this.values.map { it.first.name }.joinToString(",")}]"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is UpdatePhysicalOperatorNode) return false

        if (entity != other.entity) return false
        if (values != other.values) return false

        return true
    }

    override fun hashCode(): Int {
        var result = entity.hashCode()
        result = 31 * result + values.hashCode()
        return result
    }
}