package org.vitrivr.cottontail.core.queries.nodes

import org.vitrivr.cottontail.core.queries.planning.cost.Cost

/**
 * A [Node] that incurs a [Cost] on the query plan.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface NodeWithCost: Node {
    /** The atomic [Cost] of this [Node]. */
    val cost: Cost
}