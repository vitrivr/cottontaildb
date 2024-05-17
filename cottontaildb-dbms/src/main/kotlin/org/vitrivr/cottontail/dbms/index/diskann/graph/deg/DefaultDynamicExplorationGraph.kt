package org.vitrivr.cottontail.dbms.index.diskann.graph.deg

import com.github.benmanes.caffeine.cache.Caffeine
import com.github.benmanes.caffeine.cache.LoadingCache
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.functions.VectorisableFunction
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.index.basic.IndexMetadata.Companion.storeName
import org.vitrivr.cottontail.dbms.index.diskann.graph.DEGIndex
import org.vitrivr.cottontail.dbms.index.diskann.graph.DEGIndexConfig
import org.vitrivr.cottontail.dbms.index.diskann.graph.primitives.Node
import org.vitrivr.cottontail.dbms.index.diskann.graph.serializer.TupleIdNodeSerializer
import org.vitrivr.cottontail.dbms.index.pq.PQIndex
import org.vitrivr.cottontail.utilities.graph.undirected.WeightedUndirectedInMemoryGraph


/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
@Suppress("UNCHECKED_CAST")
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

    /** The Xodus [Store] used to store [DefaultDynamicExplorationGraph]s. */
    private val store = this.index.xodusTx.environment.openStore(index.dbo.name.storeName(), StoreConfig.WITH_DUPLICATES, index.xodusTx, false)
        ?: throw DatabaseException.DataCorruptionException("Data store for index ${index.dbo.name} is missing.")

    /** The [WeightedUndirectedInMemoryGraph] backing this [DefaultDynamicExplorationGraph]. */
    override val graph = WeightedUndirectedInMemoryGraph<Node<TupleId>>()

    /** An internal [LoadingCache]*/
    private val vectorCache: LoadingCache<Node<TupleId>,V> = Caffeine.newBuilder()
        .maximumWeight(1_000_000_000)
        .weigher { _: Node<TupleId>, value: V -> value.type.physicalSize }
        .build { node: Node<TupleId> ->
            (this.index.parent.read(node.label)[this.index.columns[0]] as? V) ?: throw DatabaseException.DataCorruptionException("Could not find value for node $node.")
        }

    init {
        this.graph.readFromStore(store, this.xodusTx, TupleIdNodeSerializer())
    }

    override fun storeValue(node: Node<TupleId>, value: V) = this.vectorCache.put(node, value)
    override fun getValue(node: Node<TupleId>): V = this.vectorCache[node]
    override fun distance(a: V, b: V): Float =  this.distanceFunction.invoke(a, b)!!.value.toFloat()
}