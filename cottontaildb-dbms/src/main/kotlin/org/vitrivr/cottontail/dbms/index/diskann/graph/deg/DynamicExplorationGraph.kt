package org.vitrivr.cottontail.dbms.index.diskann.graph.deg

import org.vitrivr.cottontail.core.basics.CloseableIterator
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.dbms.index.diskann.graph.primitives.Distance
import org.vitrivr.cottontail.dbms.index.diskann.graph.primitives.Node
import org.vitrivr.cottontail.utilities.graph.Graph

/**
 * This class outlines a Dynamic Exploration Graph (DEG) as proposed in [1]. It can be used to perform approximate nearest neighbour search (ANNS).
 *
 * Literature:
 * [1] Hezel, Nico, et al. "Fast Approximate Nearest Neighbor Search with a Dynamic Exploration Graph using Continuous Refinement." arXiv preprint arXiv:2307.10479 (2023)
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface DynamicExplorationGraph<I:Comparable<I>,V>: Graph<Node<I>> {
    /** The maximum degree of [Node]s* in this [DynamicExplorationGraph]. */
    val degree: Int

    /**
     * This method indexes a new entry consisting of an identifier [I] and a value [V] into this [DynamicExplorationGraph].
     *
     * @param identifier The identifier [I] of the entry to index.
     * @param value The value [V] of the entry to index.
     */
    fun index(identifier: I, value: V)

    /**
     * Performs a search in this [AbstractDynamicExplorationGraph].
     *
     * @param query The query [VectorValue] to search for.
     * @param k The number of nearest neighbours to return.
     * @param eps The epsilon value for the search.
     * @return [List] of [Triple]s containing the [Distance] elements for the nearest neighbours.
     */
    fun search(query: V, k: Int, eps: Float): List<Distance<I>>

    /**
     * Performs a search in this [AbstractDynamicExplorationGraph].
     *
     * @param query The query [VectorValue] to search for.
     * @param k The number of nearest neighbours to return.
     * @return [List] of [Triple]s containing the [Distance] elements for the nearest neighbours.
     */
    fun search(query: V, k: Int, eps: Float, seeds: List<Node<I>>): List<Distance<I>>

    /**
     * Generates and returns a [CloseableIterator] for all vertices [V] in this [DynamicExplorationGraph].
     *
     * @return [CloseableIterator] over all [Node]s.
     */
    override fun vertices(): CloseableIterator<Node<I>>

    /**
     * Returns the value [V] for the given [Node].
     *
     * @param node The [Node] for which to load the value [V]
     * @return The value [V]
     */
    fun getValue(node: Node<I>): V

    /**
     * Stores the value [V] for the given [Node].
     *
     * @param node The [Node] for which to store the value [V]
     * @return The value [V]
     */
    fun storeValue(node: Node<I>, value: V)
}