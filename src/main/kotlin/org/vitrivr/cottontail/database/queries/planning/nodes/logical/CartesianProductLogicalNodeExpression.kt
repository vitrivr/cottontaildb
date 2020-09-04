package org.vitrivr.cottontail.database.queries.planning.nodes.logical

import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression

/**
 * A [LogicalNodeExpression] that formalizes application of a cartesian product between two
 * input [org.vitrivr.cottontail.model.recordset.Recordset]s. Used for join operations.
 *
 * Since the cartesian product always takes two inputs, the input arity is alway two.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class CartesianProductLogicalNodeExpression : NodeExpression.LogicalNodeExpression() {

    /** Input arity of [CartesianProductLogicalNodeExpression] is always two. */
    override val inputArity: Int = 2

    /**
     * Returns a copy of this [KnnLogicalNodeExpression]
     *
     * @return Copy of this [KnnLogicalNodeExpression]
     */
    override fun copy(): CartesianProductLogicalNodeExpression = CartesianProductLogicalNodeExpression()
}