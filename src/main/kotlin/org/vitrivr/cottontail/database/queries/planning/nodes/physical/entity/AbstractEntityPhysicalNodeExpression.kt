package org.vitrivr.cottontail.database.queries.planning.nodes.physical.entity

import org.vitrivr.cottontail.database.queries.planning.nodes.interfaces.NodeExpression

/**
 * An abstract [NodeExpression.PhysicalNodeExpression] implementation that acts on a Cottontail DB
 * [org.vitrivr.cottontail.database.entity.Entity] and fetches data from it.
 *
 * @author Ralph Gasser
 * @version 1.0
 */
abstract class AbstractEntityPhysicalNodeExpression : NodeExpression.PhysicalNodeExpression() {
    /** The arity of the [AbstractEntityPhysicalNodeExpression] is always on. */
    override val inputArity = 0
}