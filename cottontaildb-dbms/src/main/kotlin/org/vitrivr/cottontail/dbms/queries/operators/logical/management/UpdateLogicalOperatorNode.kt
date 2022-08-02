package org.vitrivr.cottontail.dbms.queries.operators.logical.management

import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.management.UpdateOperator
import org.vitrivr.cottontail.dbms.queries.operators.basics.OperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.basics.UnaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.management.UpdatePhysicalOperatorNode

/**
 * A [DeleteLogicalOperatorNode] that formalizes an UPDATE operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 2.3.0
 */
class UpdateLogicalOperatorNode(input: Logical, val entity: EntityTx, val values: List<Pair<ColumnDef<*>, Binding>>) : UnaryLogicalOperatorNode(input) {

    companion object {
        private const val NODE_NAME = "Update"
    }

    /** The name of this [InsertLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [UpdateLogicalOperatorNode] does produce the columns defined in the [UpdateOperator]. */
    override val columns: List<ColumnDef<*>> = UpdateOperator.COLUMNS

    /** The [UpdateLogicalOperatorNode] requires the [ColumnDef] that are being updated. */
    override val requires: List<ColumnDef<*>> = this.values.map { it.first }

    /**
     * Creates a copy of this [UpdateLogicalOperatorNode].
     *
     * @param input The new input [OperatorNode.Logical]
     * @return Copy of this [UpdateLogicalOperatorNode]
     */
    override fun copyWithNewInput(vararg input: Logical): UpdateLogicalOperatorNode {
        require(input.size == 1) { "The input arity for UpdateLogicalOperatorNode.copyWithNewInput() must be 1 but is ${input.size}. This is a programmer's error!"}
        return UpdateLogicalOperatorNode(input = input[0], entity = this.entity, values = this.values)
    }

    /**
     * Returns a [UpdatePhysicalOperatorNode] representation of this [UpdateLogicalOperatorNode]
     *
     * @return [UpdatePhysicalOperatorNode]
     */
    override fun implement() = UpdatePhysicalOperatorNode(this.input.implement(), this.entity, this.values)

    override fun toString(): String = "${super.toString()}[${this.values.map { it.first.name }.joinToString(",")}]"

    /**
     * Generates and returns a [Digest] for this [UpdateLogicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest {
        var result = this.entity.dbo.name.hashCode().toLong()
        result += 33L * result + this.values.hashCode()
        return result
    }
}