package org.vitrivr.cottontail.dbms.index.diskann.graph

import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.vitrivr.cottontail.utilities.graph.Graph
import org.vitrivr.cottontail.utilities.graph.memory.InMemoryGraph

/**
 *
 */
class InMemoryDynamicExplorationGraph<I,V>(degree: Int, private val df: (V, V) -> Double): AbstractDynamicExplorationGraph<I,V>(degree, InMemoryGraph(degree)) {
    private val vectors = Object2ObjectOpenHashMap<I,V>()
    override fun size(): Long = this.graph.size()
    override fun distance(a: V, b: V): Double = this.df(a, b)
    override fun loadVector(identifier: I): V = this.vectors[identifier] ?: throw NoSuchElementException("Could not find vector for identifier $identifier")
    override fun storeVector(identifier: I, vector: V) {
        this.vectors[identifier] = vector
    }
}