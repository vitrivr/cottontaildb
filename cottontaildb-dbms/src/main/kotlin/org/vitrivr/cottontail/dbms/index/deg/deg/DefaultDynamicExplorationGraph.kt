package org.vitrivr.cottontail.dbms.index.deg.deg

import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.LoadingCache
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.FloatBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.functions.VectorisableFunction
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.index.basic.IndexMetadata.Companion.storeName
import org.vitrivr.cottontail.dbms.index.deg.DEGIndex
import org.vitrivr.cottontail.dbms.index.deg.DEGIndexConfig
import org.vitrivr.cottontail.dbms.index.deg.primitives.Node
import org.vitrivr.cottontail.dbms.index.deg.serializer.TupleIdNodeSerializer
import org.vitrivr.cottontail.dbms.index.pq.PQIndex
import org.vitrivr.cottontail.utilities.graph.Edge
import org.vitrivr.cottontail.utilities.graph.undirected.WeightedUndirectedInMemoryGraph
import java.io.ByteArrayInputStream

/**
 * An [AbstractDynamicExplorationGraph] backed by a [DEGIndex.Tx] object.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class DefaultDynamicExplorationGraph<V: VectorValue<*>>(config: DEGIndexConfig, val index: DEGIndex.Tx): AbstractDynamicExplorationGraph<TupleId, V>(config.degree, config.kExt, config.epsilonExt) {
    /** The [VectorDistance] function employed by this [PQIndex]. */
    val distanceFunction: VectorDistance<*> by lazy {
        val signature = Signature.Closed(config.distance, arrayOf(index.columns[0].type, index.columns[0].type), Types.Double)
        val dist = this.index.context.catalogue.functions.obtain(signature) as VectorDistance<*>
        if (dist is VectorisableFunction<*>) {
            dist.vectorized() as VectorDistance<*>
        } else {
            dist
        }
    }

    /** The Xodus transaction used by this [DefaultDynamicExplorationGraph]. */
    private val xodusTx = this.index.xodusTx

    /** Flag indicating, that changes were made to this [DefaultDynamicExplorationGraph]. */
    private var dirty: Boolean = false

    /** An internal [LoadingCache]*/
    @Suppress("UNCHECKED_CAST")
    private val vectorCache: LoadingCache<Node<TupleId>, V> = Caffeine.newBuilder()
        .maximumWeight(100_000_000)
        .weigher { _: Node<TupleId>, value: V -> value.type.physicalSize }
        .build { node: Node<TupleId> ->
            (this.index.parent.read(node.label)[this.index.columns[0]] as? V) ?: throw DatabaseException.DataCorruptionException("Could not find value for node $node.")
        }

    /** The [WeightedUndirectedInMemoryGraph] backing this [DefaultDynamicExplorationGraph]. */
    override val graph by lazy {
        this.load()
    }

    /**
     * Stores a [Node] <-> [VectorValue] pair in this [vectorCache].
     *
     * Used during indexing.
     *
     * @param node The [Node] to store the value for.
     * @param value The [VectorValue] to store.
     */
    override fun storeValue(node: Node<TupleId>, value: V) {
        this.vectorCache.put(node, value)
    }

    /**
     * Retrieves a [VectorValue] for the given [Node] from the [vectorCache].
     *
     * @param node The [Node] to store the value for.
     * @return The [VectorValue] for the given [Node].
     */
    override fun getValue(node: Node<TupleId>): V = this.vectorCache[node]

    /**
     * Updates the [dirty] flag of this [DefaultDynamicExplorationGraph] and indexes the value.
     */
    override fun index(identifier: TupleId, value: V) {
        super.index(identifier, value)
        this.dirty = true
    }

    /**
     * Obtains the distance between [a] and [b].
     *
     * @param a The first [VectorValue] of type [V].
     * @param b The second [VectorValue] of type [V].
     * @return The distance between [a] and [b].
     */
    override fun distance(a: V, b: V): Float =  this.distanceFunction.invoke(a, b)!!.value.toFloat()

    /**
     * Loads this [DefaultDynamicExplorationGraph] from the [Store] backing the [DEGIndex].
     *
     * @return [WeightedUndirectedInMemoryGraph]
     */
    private fun load(): WeightedUndirectedInMemoryGraph<Node<TupleId>> {
        /* Obtain environment and store name. */
        val storeName = this.index.dbo.name.storeName()
        val environment = this.index.xodusTx.environment

        /* Open store and prepare empty adjacency list. */
        val store = environment.openStore(storeName, StoreConfig.WITH_DUPLICATES, this.xodusTx)
        val adjacencyList = Object2ObjectOpenHashMap<Node<TupleId>,MutableList<Edge<Node<TupleId>>>>()

        /* Prepare serializer and read from store. */
        val serializer = TupleIdNodeSerializer()
        store.openCursor(this.xodusTx).use {
            while (it.nextNoDup) {
                val key = serializer.deserialize(it.key)
                val edges = ArrayList<Edge<Node<TupleId>>>(this@DefaultDynamicExplorationGraph.degree)
                do {
                    val value = it.value
                    if (value != ByteIterable.EMPTY) {
                        val input = ByteArrayInputStream(value.bytesUnsafe, 0, value.length)
                        val to = serializer.read(input)
                        val weight = FloatBinding.BINDING.readObject(input)
                        edges.add(Edge(to, weight))
                    }
                } while (it.nextDup)
                adjacencyList[key] = edges
            }
        }

        return WeightedUndirectedInMemoryGraph(this.degree, adjacencyList)
    }

    /**
     * Saves this [DefaultDynamicExplorationGraph] and writes the changes to the [Store] backing the [DEGIndex].
     *
     * The index is only persisted if there have been changes to it.
     */
    fun save() {
        if (this.dirty) {
            /* Obtain environment and store name. */
            val storeName = this.index.dbo.name.storeName()
            val environment = this.index.xodusTx.environment

            /* Truncate and re-open store. */
            environment.truncateStore(storeName, this.xodusTx)
            val store = environment.openStore(storeName, StoreConfig.WITH_DUPLICATES, this.xodusTx)

            /* Prepare serializer and write to store. */
            val serializer = TupleIdNodeSerializer()
            for (vertex in this.graph.vertices()) {
                val key = serializer.serialize(vertex)
                store.delete(this.index.xodusTx, key)

                /* Obtain and serialize edges. */
                val edges = this.graph.edges(vertex)
                if (edges.isEmpty()) {
                    store.add(this.index.xodusTx, key, ByteIterable.EMPTY)
                } else {
                    for ((edge, weight) in edges) {
                        val out = LightOutputStream()
                        serializer.write(edge, out)
                        FloatBinding.BINDING.writeObject(out, weight)
                        store.put(this.index.xodusTx, key, out.asArrayByteIterable())
                    }
                }
            }
            this.dirty = false
        }
    }
}