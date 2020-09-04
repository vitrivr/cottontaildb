package org.vitrivr.cottontail.database.queries.planning.nodes.logical

import org.vitrivr.cottontail.database.queries.components.BooleanPredicate
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression

/**
 * A [LogicalNodeExpression] that formalizes filtering using some [BooleanPredicate].
 * Filters always operate on a single input and  thus have an input arity of 1.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class FilterLogicalNodeExpression(val predicate: BooleanPredicate) : NodeExpression.LogicalNodeExpression() {
    override val inputArity: Int = 1

    /**
     * Returns a copy of this [KnnLogicalNodeExpression]
     *
     * @return Copy of this [KnnLogicalNodeExpression]
     */
    override fun copy(): FilterLogicalNodeExpression = FilterLogicalNodeExpression(this.predicate)
}