package org.vitrivr.cottontail.dbms.index.diskann.graph

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.vitrivr.cottontail.utilities.graph.undirected.WeightedUndirectedInMemoryGraph

/**
 *
 */
class InMemoryDynamicExplorationGraph<I: Comparable<I>,V>(degree: Int, private val df: (V, V) -> Float): AbstractDynamicExplorationGraph<I,V>(degree, WeightedUndirectedInMemoryGraph(degree)) {
    private val vectors = Object2ObjectOpenHashMap<I,V>()
    override fun size(): Long = this.graph.size()
    override fun distance(a: V, b: V): Float = this.df(a, b)
    override fun loadVector(identifier: I): V = this.vectors[identifier] ?: throw NoSuchElementException("Could not find vector for identifier $identifier")
    override fun storeVector(identifier: I, vector: V) {
        this.vectors[identifier] = vector
    }
}