package org.vitrivr.cottontail.dbms.index.deg

import it.unimi.dsi.fastutil.longs.Long2ObjectFunction
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.binding.MissingTuple
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.euclidean.EuclideanDistance
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.entity.values.StoredTuple
import org.vitrivr.cottontail.dbms.entity.values.StoredValue
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.index.basic.*
import org.vitrivr.cottontail.dbms.index.basic.IndexMetadata.Companion.storeName
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AsyncIndexRebuilder
import org.vitrivr.cottontail.dbms.index.deg.deg.DefaultDynamicExplorationGraph
import org.vitrivr.cottontail.dbms.index.deg.rebuilder.DEGIndexRebuilder
import org.vitrivr.cottontail.dbms.index.lucene.LuceneIndex
import org.vitrivr.cottontail.dbms.index.pq.rebuilder.AsyncPQIndexRebuilder
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.server.Instance

/**
 * An [AbstractIndex] structure for proximity based queries that uses the [DynamicExplorationGraph] (DEG) algorithm described in [1].
 *
 * Can be used for all type of [VectorValue]s and distance metrics.
 *
 * References:
 * [1] Hezel, Nico, et al. "Fast Approximate Nearest Neighbor Search with a Dynamic Exploration Graph using Continuous Refinement." arXiv preprint arXiv:2307.10479 (2023)
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class DEGIndex(name: Name.IndexName, parent: DefaultEntity) : AbstractIndex(name, parent) {

    companion object: IndexDescriptor<DEGIndex> {
        /** [Logger] instance used by [DEGIndex]. */
        private val LOGGER: Logger = LoggerFactory.getLogger(DEGIndex::class.java)

        override val supportsIncrementalUpdate: Boolean = true
        override val supportsAsyncRebuild: Boolean = true
        override val supportsPartitioning: Boolean = false

        /**
         * Opens a [DEGIndex] for the given [Name.IndexName] in the given [DefaultEntity].
         *
         * @param name The [Name.IndexName] of the [DEGIndex].
         * @param entity The [Entity] that holds the [DEGIndex].
         * @return The opened [DEGIndex]
         */
        override fun open(name: Name.IndexName, entity: Entity): DEGIndex = DEGIndex(name, entity as DefaultEntity)

        /**
         * Initializes the [Store] for a [DEGIndex].
         *
         * @param name The [Name.IndexName] of the [DEGIndex].
         * @param parent The [EntityTx] that requested index initialization.
         * @return True on success, false otherwise.
         */
        override fun initialize(name: Name.IndexName, parent: EntityTx): Boolean = try {
            require(parent is DefaultEntity.Tx) { "DEGIndex can only be used with DefaultEntity.Tx" }
            parent.xodusTx.environment.openStore(name.storeName(), StoreConfig.WITH_DUPLICATES, parent.xodusTx, true) != null
        } catch (e:Throwable) {
            LOGGER.error("Failed to initialize DEG index $name due to an exception: ${e.message}.")
            false
        }

        /**
         * De-initializes the [Store] for associated with a [DEGIndex].
         *
         * @param name The [Name.IndexName] of the [LuceneIndex].
         * @param parent The [EntityTx] that requested index de-initialization.

         * @return True on success, false otherwise.
         */
        override fun deinitialize(name: Name.IndexName, parent: EntityTx): Boolean = try {
            require(parent is DefaultEntity.Tx) { "DEGIndex can only be used with DefaultEntity.Tx" }
            parent.xodusTx.environment.removeStore(name.storeName(),  parent.xodusTx)
            true
        } catch (e:Throwable) {
            LOGGER.error("Failed to de-initialize DEG index $name due to an exception: ${e.message}.")
            false
        }

        /**
         * Generates and returns a [DEGIndexConfig] for the given [parameters] (or default values, if [parameters] are not set).
         *
         * @param parameters The parameters to initialize the default [DEGIndexConfig] with.
         */
        override fun buildConfig(parameters: Map<String, String>) = DEGIndexConfig(
            parameters[DEGIndexConfig.KEY_DISTANCE]?.let { Name.FunctionName.create(it) } ?: EuclideanDistance.FUNCTION_NAME,
            parameters[DEGIndexConfig.KEY_DEGREE]?.toIntOrNull() ?: DEGIndexConfig.DEFAULT_DEGREE,
            parameters[DEGIndexConfig.KEY_EPSILON_EXT]?.toFloatOrNull() ?: DEGIndexConfig.DEFAULT_EPSILON_EXT,
            parameters[DEGIndexConfig.KEY_K_EXT]?.toIntOrNull() ?: ((parameters[DEGIndexConfig.KEY_DEGREE]?.toIntOrNull() ?: DEGIndexConfig.DEFAULT_DEGREE) * 2)
        )

        /**
         * Returns the [DEGIndexConfig.Binding]
         *
         * @return [DEGIndexConfig.Binding]
         */
        override fun configBinding() = DEGIndexConfig.Binding
    }

    override val type: IndexType = IndexType.DEG

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [DEGIndex].
     *
     * @param parent If parent [EntityTx] this [IndexTx] belongs to.
     */
    override fun newTx(parent: EntityTx): IndexTx {
        require(parent is DefaultEntity.Tx) { "DEGIndex can only be used with DefaultEntity.Tx" }
        return this.transactions.computeIfAbsent(parent.context.txn.transactionId, Long2ObjectFunction {
            val subTransaction = Tx(parent)
            parent.context.txn.registerSubtransaction(subTransaction)
            subTransaction
        })
    }

    /**
     * Opens and returns a new [DEGIndexRebuilder] object that can be used to rebuild with this [DEGIndex].
     *
     * @param context The [QueryContext] to create [DEGIndexRebuilder] for.
     * @return [DEGIndexRebuilder]
     */
    override fun newRebuilder(context: QueryContext) = DEGIndexRebuilder(this, context)

    /**
     * Opens and returns a new [AsyncPQIndexRebuilder] object that can be used to rebuild with this [DEGIndex].
     *
     * @param instance The [Instance] that requested the [AsyncIndexRebuilder].
     * @return [AsyncPQIndexRebuilder]
     */
    override fun newAsyncRebuilder(instance: Instance): AsyncIndexRebuilder<*> {
        throw UnsupportedOperationException("DEGIndex does not support asynchronous rebuilding.")
    }

    /**
     * A [IndexTx] that affects this [AbstractIndex].
     */
    inner class Tx(parent: DefaultEntity.Tx) : AbstractIndex.Tx(parent) {
        /** Constructs a [DefaultDynamicExplorationGraph] for this [DEGIndex]. */
        internal val graph = DefaultDynamicExplorationGraph<VectorValue<*>>(this.config as DEGIndexConfig, this)

        /**
         * Checks if this [DEGIndex] can process the provided [Predicate] and returns true if so and false otherwise.
         *
         * @param predicate [Predicate] to check.
         * @return True if [Predicate] can be processed, false otherwise.
         */
        @Synchronized
        override fun canProcess(predicate: Predicate): Boolean {
            if (predicate !is ProximityPredicate.NNS) return false
            if (predicate.column.physical == this.columns[0]) return false
            if (predicate.distance.name == this.graph.distanceFunction.name) return false
            return true
        }

        /**
         * Calculates the count estimate of this [DEGIndex.Tx] processing the provided [Predicate].
         *
         * @param predicate [Predicate] to check.
         * @return Count estimate for the [Predicate]
         */
        @Synchronized
        override fun countFor(predicate: Predicate): Long {
            if (!canProcess(predicate)) return 0L
            return (predicate as ProximityPredicate.NNS).k
        }

        /**
         * Calculates the cost estimate of this [DEGIndex.Tx] processing the provided [Predicate].
         *
         * @param predicate [Predicate] to check.
         * @return [Cost] estimate for the [Predicate]
         */
        @Synchronized
        override fun costFor(predicate: Predicate): Cost {
            if (!canProcess(predicate)) return Cost.INVALID
            return Cost(
                io = Cost.DISK_ACCESS_READ_SEQUENTIAL.io * Short.SIZE_BYTES,
                cpu = 4 * Cost.MEMORY_ACCESS.cpu + Cost.FLOP.cpu,
                accuracy = 0.2f
            )
        }

        /**
         * Returns a [List] of the [ColumnDef] produced by this [DEGIndex].
         *
         * @return [List] of [ColumnDef].
         */
        @Synchronized
        override fun columnsFor(predicate: Predicate): List<ColumnDef<*>> {
            require(predicate is ProximityPredicate.NNS) { "PQIndex can only process proximity search." }
            return this.parent.listColumns() + predicate.distanceColumn.column
        }

        /**
         * Returns the map of [Trait]s this [DEGIndex] implements for the given [Predicate]s.
         *
         * @param predicate [Predicate] to check.
         * @return Map of [Trait]s for this [DEGIndex]
         */
        @Synchronized
        override fun traitsFor(predicate: Predicate): Map<TraitType<*>, Trait> = when (predicate) {
            is ProximityPredicate.NNS -> mutableMapOf()
            else -> throw IllegalArgumentException("Unsupported predicate for high-dimensional index. This is a programmer's error!")
        }

        /**
         * Performs a lookup through this [DEGIndex.Tx] and returns a [Cursor] of all [Tuple]s that match the [Predicate].
         * Only supports [ProximityPredicate.NNS]s.
         *
         * <strong>Important:</strong> The [Iterator] is not thread safe! It remains to the
         * caller to close the [Iterator]
         *
         * @param predicate The [Predicate] for the lookup
         * @return The resulting [Iterator]
         */
        @Synchronized
        override fun filter(predicate: Predicate): Cursor<Tuple> {
            require(predicate is ProximityPredicate.NNS) { "DEGIndex can only process proximity search." }
            val query = with(MissingTuple) {
                with(this@Tx.context.bindings) {
                    predicate.query.getValue() as VectorValue<*>
                }
            }
            return DEGIndexCursor(query, predicate.k.toInt(), this.columnsFor(predicate).toTypedArray(), this)
        }

        /**
         *
         */
        override fun filter(predicate: Predicate, partition: LongRange): Cursor<Tuple> {
            throw UnsupportedOperationException("DEGIndex does not support partitioning.")
        }

        /**
         * Returns the number of entries in this [DEGIndex].
         *
         * @return Number of entries in this [DEGIndex]
         */
        @Synchronized
        override fun count(): Long = this.graph.size.toLong()

        /**
         *
         */
        @Synchronized
        override fun tryApply(event: DataEvent.Insert): Boolean {
            TODO("Not yet implemented")
        }

        /**
         *
         */
        @Synchronized
        override fun tryApply(event: DataEvent.Update): Boolean {
            TODO("Not yet implemented")
        }

        /**
         *
         */
        @Synchronized
        override fun tryApply(event: DataEvent.Delete): Boolean {
            TODO("Not yet implemented")
        }
    }
}