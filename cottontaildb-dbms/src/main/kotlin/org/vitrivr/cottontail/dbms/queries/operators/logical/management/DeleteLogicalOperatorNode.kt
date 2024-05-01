package org.vitrivr.cottontail.dbms.queries.operators.logical.management

import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.management.DeleteOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.management.DeletePhysicalOperatorNode

/**
 * A [DeleteLogicalOperatorNode] that formalizes a DELETE operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 2.9.0
 */
class DeleteLogicalOperatorNode(input: Logical, val context: QueryContext, val entity: EntityTx) : UnaryLogicalOperatorNode(input) {

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

    /**
     * Creates a copy of this [DeleteLogicalOperatorNode].
     *
     * @param input The new input [OperatorNode.Logical]
     * @return Copy of this [DeleteLogicalOperatorNode]
     */
    override fun copyWithNewInput(vararg input: Logical): DeleteLogicalOperatorNode {
        require(input.size == 1) { "The input arity for DeleteLogicalOperatorNode.copyWithNewInput() must be 1 but is ${input.size}. This is a programmer's error!"}
        return DeleteLogicalOperatorNode(input = input[0], context = context, entity = this.entity)
    }

    /**
     * Returns a [DeletePhysicalOperatorNode] representation of this [DeleteLogicalOperatorNode]
     *
     * @return [DeletePhysicalOperatorNode]
     */
    override fun implement() = DeletePhysicalOperatorNode(this.input.implement(), this.context, this.entity)

    override fun toString(): String = "${super.toString()}[${this.entity.dbo.name}]"

    /**
     * Generates and returns a [Digest] for this [DeleteLogicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest = this.entity.dbo.name.hashCode().toLong() + -5L
}