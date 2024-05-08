package org.vitrivr.cottontail.dbms.index.diskann.graph.deg

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.Transaction
import org.vitrivr.cottontail.dbms.index.diskann.graph.primitives.Node
import org.vitrivr.cottontail.utilities.graph.MutableGraph
import org.vitrivr.cottontail.utilities.graph.undirected.VertexSerializer
import org.vitrivr.cottontail.utilities.graph.undirected.WeightedUndirectedXodusGraph

/**
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class XodusDynamicExplorationGraph<I: Comparable<I>,V>(degree: Int, kExt: Int, epsilonExt: Float, store: Store, txn: Transaction, serializer: VertexSerializer<Node<I>>, private val df: (V, V) -> Float): AbstractDynamicExplorationGraph<I, V>(degree, kExt, epsilonExt) {
    private val vectors = Object2ObjectOpenHashMap<Node<I>,V>()
    override val graph: MutableGraph<Node<I>> = WeightedUndirectedXodusGraph(store, txn, serializer)
    override fun distance(a: V, b: V): Float = this.df(a, b)
    override fun getValue(node: Node<I>): V = this.vectors[node] ?: throw NoSuchElementException("Could not find value for node $node.")
    override fun storeValue(node: Node<I>, value: V) {
        this.vectors[node] = value
    }
}