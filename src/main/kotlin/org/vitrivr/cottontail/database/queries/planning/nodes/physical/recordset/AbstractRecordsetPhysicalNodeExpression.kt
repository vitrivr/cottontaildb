package org.vitrivr.cottontail.database.queries.planning.nodes.physical.recordset

import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression

/**
 * An abstract [PhysicalNodeExpression] implementation that acts on [org.vitrivr.cottontail.model.recordset.Recordset]s as input.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
abstract class AbstractRecordsetPhysicalNodeExpression : NodeExpression.PhysicalNodeExpression() {
    /** The arity of the [AbstractRecordsetPhysicalNodeExpression] is always on. */
    override val inputArity = 1

    /** Reference to the input [PhysicalNodeExpression]. */
    val input: PhysicalNodeExpression
        get() {
            val input = this.inputs.getOrNull(0)
            if (input is PhysicalNodeExpression) {
                return input
            } else {
                throw Exception()
            }
        }
}