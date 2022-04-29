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
     * Creates a copy of this [Node]. The copy must be built in such a ways, that all relevant data structures
     * that may be accessed concurrently, are copied.
     *
     * @return Copy of this [Node]
     */
    fun copy(): Node

    /**
     * Calculates and returns the digest for this [Node]. The digest is similar
     * to [hashCode] and follows similar semantics: If two [Node]s are considered equal
     * in the eye of any query execution component, it should return the same digest.
     *
     * Used for caching and re-use of query plans.
     *
     * @return  [Digest] of this [Node]
     */
    fun digest(): Digest
}