package org.vitrivr.cottontail.database.queries.planning.nodes.logical

import org.vitrivr.cottontail.database.queries.components.KnnPredicate
import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression

/**
 * A [NodeExpression.LogicalNodeExpression] that formalizes the execution of a kNN query
 * on a [org.vitrivr.cottontail.model.recordset.Recordset].
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class KnnLogicalNodeExpression(val predicate: KnnPredicate<*>) : NodeExpression.LogicalNodeExpression() {
    /** Input arity of [KnnLogicalNodeExpression] is always one, since it acts on a single [org.vitrivr.cottontail.model.recordset.Recordset]. */
    override val inputArity: Int = 1

    /**
     * Returns a copy of this [KnnLogicalNodeExpression]
     *
     * @return Copy of this [KnnLogicalNodeExpression]
     */
    override fun copy(): KnnLogicalNodeExpression = KnnLogicalNodeExpression(this.predicate)
}