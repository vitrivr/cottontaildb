package org.vitrivr.cottontail.dbms.queries.operators.physical.management

import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.nodes.traits.NotPartitionableTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.management.UpdateOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.UnaryPhysicalOperatorNode

/**
 * A [UpdatePhysicalOperatorNode] that formalizes a UPDATE operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 2.3.0
 */
class UpdatePhysicalOperatorNode(input: Physical, val entity: EntityTx, val values: List<Pair<ColumnDef<*>, Binding>>) : UnaryPhysicalOperatorNode(input) {
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
    context(BindingContext,Record)    override val cost: Cost
        get() = ((Cost.DISK_ACCESS_WRITE + Cost.MEMORY_ACCESS) * (this.input.columns.sumOf { this.statistics[it]!!.avgWidth })) * (this.input.outputSize )

    /** The [UpdatePhysicalOperatorNode] cannot be partitioned. */
    override val traits: Map<TraitType<*>, Trait> = mapOf(NotPartitionableTrait to NotPartitionableTrait)

    /**
     * Creates and returns a copy of this [UpdatePhysicalOperatorNode] using the given parents as input.
     *
     * @param input The [OperatorNode.Physical]s that act as input.
     * @return Copy of this [UpdatePhysicalOperatorNode].
     */
    override fun copyWithNewInput(vararg input: Physical): UpdatePhysicalOperatorNode {
        require(input.size == 1) { "The input arity for UpdatePhysicalOperatorNode.copyWithNewInput() must be 1 but is ${input.size}. This is a programmer's error!"}
        return UpdatePhysicalOperatorNode(input = input[0], entity = this.entity, values = this.values)
    }

    /**
     * Converts this [UpdatePhysicalOperatorNode] to a [UpdateOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
     override fun toOperator(ctx: QueryContext) = UpdateOperator(this.input.toOperator(ctx), this.entity, this.values.map { it.first to it.second }, ctx)

    /**
     * The [UpdatePhysicalOperatorNode] cannot be partitioned.
     */
    override fun tryPartition(ctx: QueryContext, max: Int): Physical? = null

    override fun toString(): String = "${super.toString()}[${this.values.map { it.first.name }.joinToString(",")}]"

    /**
     * Generates and returns a [Digest] for this [UpdatePhysicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest {
        var result = this.entity.dbo.name.hashCode().toLong()
        result += 31L * result + this.values.hashCode()
        return result
    }
}