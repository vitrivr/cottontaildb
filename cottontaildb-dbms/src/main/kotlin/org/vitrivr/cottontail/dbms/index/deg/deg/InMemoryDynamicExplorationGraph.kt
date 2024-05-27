package org.vitrivr.cottontail.dbms.index.deg.deg

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.vitrivr.cottontail.dbms.index.deg.primitives.Node
import org.vitrivr.cottontail.utilities.graph.MutableGraph
import org.vitrivr.cottontail.utilities.graph.undirected.WeightedUndirectedInMemoryGraph

/**
 *
 */
class InMemoryDynamicExplorationGraph<I: Comparable<I>,V>(config: DEGConfig, private val df: (V, V) -> Float): AbstractDynamicExplorationGraph<I, V>(config) {
    private val vectors = Object2ObjectOpenHashMap<Node<I>,V>()
    override val graph: MutableGraph<Node<I>> = WeightedUndirectedInMemoryGraph(this.config.degree)
    override fun distance(a: V, b: V): Float = this.df(a, b)
    override fun getValue(node: Node<I>): V = this.vectors[node] ?: throw NoSuchElementException("Could not find value for node $node.")
    override fun storeValue(node: Node<I>, value: V) {
        this.vectors[node] = value
    }
}