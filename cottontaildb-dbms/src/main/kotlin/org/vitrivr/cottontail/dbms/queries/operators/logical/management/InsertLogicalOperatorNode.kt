package org.vitrivr.cottontail.dbms.queries.operators.logical.management

import org.vitrivr.cottontail.core.queries.Digest
import org.vitrivr.cottontail.core.queries.GroupId
import org.vitrivr.cottontail.core.queries.binding.Binding
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.management.InsertOperator
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.queries.operators.basics.NullaryLogicalOperatorNode
import org.vitrivr.cottontail.dbms.queries.operators.physical.management.InsertPhysicalOperatorNode

/**
 * A [InsertLogicalOperatorNode] that formalizes a INSERT operation on an [Entity].
 *
 * @author Ralph Gasser
 * @version 2.9.0
 */
class InsertLogicalOperatorNode(override val groupId: GroupId, val context: QueryContext, val entity: EntityTx, val tuples: MutableList<Tuple>) : NullaryLogicalOperatorNode() {

    companion object {
        private const val NODE_NAME = "Insert"
    }

    /** The name of this [InsertLogicalOperatorNode]. */
    override val name: String
        get() = NODE_NAME

    /** The [InsertLogicalOperatorNode] produces the columns defined in the [InsertOperator] */
    override val columns: List<Binding.Column> = InsertOperator.COLUMNS.map {
        this.context.bindings.bind(it, null)
    }

    /**
     * Creates and returns a copy of this [InsertLogicalOperatorNode] without any children or parents.
     *
     * @return Copy of this [InsertLogicalOperatorNode].
     */
    override fun copy() = InsertLogicalOperatorNode(this.groupId, this.context, this.entity, this.tuples)

    /**
     * Returns a [InsertPhysicalOperatorNode] representation of this [InsertLogicalOperatorNode]
     *
     * @return [InsertPhysicalOperatorNode]
     */
    override fun implement() = InsertPhysicalOperatorNode(this.groupId, this.context, this.entity, this.tuples)

    /**
     * Generates and returns a [Digest] for this [InsertLogicalOperatorNode].
     *
     * @return [Digest]
     */
    override fun digest(): Digest {
        var result = this.entity.dbo.name.hashCode().toLong()
        result += 33L * result + this.tuples.hashCode()
        return result
    }
}