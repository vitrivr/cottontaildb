package org.vitrivr.cottontail.dbms.index.diskann.graph.deg

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.functions.VectorisableFunction
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.dbms.column.ColumnTx
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.index.basic.IndexMetadata.Companion.storeName
import org.vitrivr.cottontail.dbms.index.diskann.graph.DEGIndex
import org.vitrivr.cottontail.dbms.index.diskann.graph.DEGIndexConfig
import org.vitrivr.cottontail.dbms.index.diskann.graph.primitives.Node
import org.vitrivr.cottontail.dbms.index.diskann.graph.serializer.TupleIdNodeSerializer
import org.vitrivr.cottontail.dbms.index.pq.PQIndex
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.utilities.graph.undirected.WeightedUndirectedInMemoryGraph
import java.lang.ref.SoftReference


/**
 *
 * @author Ralph Gasser
 * @version 1.0
 */
class DefaultDynamicExplorationGraph<V: VectorValue<*>>(config: DEGIndexConfig, index: DEGIndex.Tx, context: QueryContext): AbstractDynamicExplorationGraph<TupleId, V>(config.degree, config.kExt, config.epsilonExt) {

    /** The [VectorDistance] function employed by this [PQIndex]. */
    val distanceFunction: VectorDistance<*> by lazy {
        val signature = Signature.Closed(config.distance, arrayOf(index.columns[0].type, index.columns[0].type), Types.Double)
        val dist = context.catalogue.functions.obtain(signature) as VectorDistance<*>
        if (dist is VectorisableFunction<*>) {
            dist.vectorized() as VectorDistance<*>
        } else {
            dist
        }
    }

    /** The Xodus transaction used by this [DefaultDynamicExplorationGraph]. */
    private val xodusTx = index.xodusTx

    /** The Xodus [Store] used to store [DefaultDynamicExplorationGraph]s. */
    private val store = index.xodusTx.environment.openStore(index.dbo.name.storeName(), StoreConfig.WITH_DUPLICATES, index.xodusTx, false)
        ?: throw DatabaseException.DataCorruptionException("Data store for index ${index.dbo.name} is missing.")

    /** The [WeightedUndirectedXodusGraph] */
    override val graph = WeightedUndirectedInMemoryGraph<Node<TupleId>>()

    /** */
    private val columnTx: ColumnTx<*>

    /** */
    private val vectorCache = Object2ObjectOpenHashMap<Node<TupleId>, SoftReference<V>>()

    init {
        val entityTx = index.parent
        this.columnTx = entityTx.columnForName(index.columns[0].name).newTx(entityTx)
        this.graph.readFromStore(store, this.xodusTx, TupleIdNodeSerializer())
    }

    override fun storeValue(node: Node<TupleId>, value: V) {
        this.vectorCache[node] = SoftReference(value)
    }

    @Suppress("UNCHECKED_CAST")
    override fun getValue(node: Node<TupleId>): V = (this.columnTx.read(node.label) as? V) ?: throw DatabaseException.DataCorruptionException("Could not find value for node $node.")

    /**
     *
     */
    override fun distance(a: V, b: V): Float =  this.distanceFunction.invoke(a, b)!!.value.toFloat()
}