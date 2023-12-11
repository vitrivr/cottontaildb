package org.vitrivr.cottontail.dbms.queries.operators.physical.management

import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.nodes.traits.NotPartitionableTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.basics.Operator
import org.vitrivr.cottontail.dbms.execution.operators.management.DeleteOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.UnaryPhysicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.logical.management.DeleteLogicalOperatorNode

/**
 * A [DeletePhysicalOperatorNode] that formalizes a delete operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 2.9.0
 */
class DeletePhysicalOperatorNode(input: Physical, val context: QueryContext, val entity: EntityTx) : UnaryPhysicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Delete"
    }

    /** The name of this [DeleteLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [DeleteLogicalOperatorNode] produces the columns defined in the [DeleteOperator] */
    override val columns: List<Binding.Column> = DeleteOperator.COLUMNS.map {
        this.context.bindings.bind(it, null)
    }

    /** The [DeletePhysicalOperatorNode] produces a single record. */
    override val outputSize: Long = 1L

    /** The [Cost] of this [DeletePhysicalOperatorNode]. */
    context(BindingContext, Tuple)    override val cost: Cost
        get() = Cost.DISK_ACCESS_WRITE * this.entity.count() * this.input.outputSize

    /** The [DeletePhysicalOperatorNode] cannot be partitioned. */
    override val traits: Map<TraitType<*>, Trait> = mapOf(NotPartitionableTrait to NotPartitionableTrait)

    /**
     * Creates and returns a copy of this [DeletePhysicalOperatorNode] using the given parents as input.
     *
     * @param input The [OperatorNode.Physical]s that act as input.
     * @return Copy of this [DeletePhysicalOperatorNode].
     */
    override fun copyWithNewInput(vararg input: Physical): DeletePhysicalOperatorNode {
        require(input.size == 1) { "The input arity for DeletePhysicalOperatorNode.copyWithNewInput() must be 1 but is ${input.size}. This is a programmer's error!"}
        return DeletePhysicalOperatorNode(input = input[0], context = this.context, entity = this.entity)
    }

    /**
     * Converts this [DeletePhysicalOperatorNode] to a [DeleteOperator].
     *
     * @param ctx The [QueryContext] used for the conversion (e.g. late binding).
     */
    override fun toOperator(ctx: QueryContext): Operator = DeleteOperator(this.input.toOperator(ctx), this.entity, ctx)

    /**
     * The [DeletePhysicalOperatorNode] cannot be partitioned.
     */
    override fun tryPartition(ctx: QueryContext, max: Int): Physical? = null


    override fun toString(): String = "${super.toString()}[${this.entity.dbo.name}]"


    /**
     * Generates and returns a [Digest] for this [DeletePhysicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest = this.entity.dbo.name.hashCode().toLong() + -7L
}