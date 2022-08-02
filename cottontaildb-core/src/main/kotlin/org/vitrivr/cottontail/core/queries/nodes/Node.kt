package org.vitrivr.cottontail.core.queries.nodes

import org.vitrivr.cottontail.core.queries.Digest

/**
 * A [Node] is an object in the tree-like structure of a query plan.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
interface Node {
    /**
     * Calculates and returns the [Digest] for this [Node], which is used for caching and re-use of query plans
     *
     * The [Digest] is similar to [hashCode] and it follows similar semantics: If two [Node]s are considered
     * equal in the eye of any query execution component, it should return the same [Digest].
     *
     * @return [Digest] of this [Node]
     */
    fun digest(): Digest
}