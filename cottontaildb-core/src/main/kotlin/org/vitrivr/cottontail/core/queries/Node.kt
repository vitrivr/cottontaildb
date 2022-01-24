package org.vitrivr.cottontail.core.queries

/**
 * A [Node] is an object in the tree-like structure of a query plan, be it logical, physical or operational.
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
interface Node {
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