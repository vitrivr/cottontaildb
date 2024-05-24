package org.vitrivr.cottontail.dbms.index.pq

import it.unimi.dsi.fastutil.longs.Long2ObjectFunction
import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.binding.MissingTuple
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.euclidean.EuclideanDistance
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.RealVectorValue
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexStructuralMetadata
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.index.basic.*
import org.vitrivr.cottontail.dbms.index.basic.IndexMetadata.Companion.storeName
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AsyncIndexRebuilder
import org.vitrivr.cottontail.dbms.index.lucene.LuceneIndex
import org.vitrivr.cottontail.dbms.index.pq.quantizer.SerializableSingleStageProductQuantizer
import org.vitrivr.cottontail.dbms.index.pq.quantizer.SingleStageQuantizer
import org.vitrivr.cottontail.dbms.index.pq.rebuilder.AsyncPQIndexRebuilder
import org.vitrivr.cottontail.dbms.index.pq.rebuilder.DataCollectionUtilities
import org.vitrivr.cottontail.dbms.index.pq.rebuilder.PQIndexRebuilder
import org.vitrivr.cottontail.dbms.index.pq.signature.SPQSignature
import org.vitrivr.cottontail.dbms.index.va.VAFIndex
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.server.Instance

/**
 * An [AbstractIndex] structure for proximity based queries that uses the product quantization (PQ) algorithm described in [1].
 *
 * Can be used for all type of [VectorValue]s and distance metrics. Implements the ADC algorithm described in [1], which is well
 * suited for million-scale datasets.
 *
 * References:
 * [1] Jegou, Herve, et al. "Product Quantization for Nearest Neighbor Search." IEEE Transactions on Pattern Analysis and Machine Intelligence. 2010.
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 4.0.0
 */
class PQIndex(name: Name.IndexName, parent: DefaultEntity): AbstractIndex(name, parent) {

    /**
     * The [IndexDescriptor] for the [PQIndex].
     */
    companion object: IndexDescriptor<PQIndex> {
        /** [Logger] instance used by [PQIndex]. */
        private val LOGGER: Logger = LoggerFactory.getLogger(PQIndex::class.java)

        /** False since [PQIndex] currently doesn't support incremental updates. */
        override val supportsIncrementalUpdate: Boolean = true

        /** False since [PQIndex] doesn't support asynchronous rebuilds. */
        override val supportsAsyncRebuild: Boolean = true

        /** True since [PQIndex] supports partitioning. */
        override val supportsPartitioning: Boolean = true

        /**
         * Opens a [PQIndex] for the given [Name.IndexName] in the given [DefaultEntity].
         *
         * @param name The [Name.IndexName] of the [PQIndex].
         * @param entity The [Entity] that holds the [PQIndex].
         * @return The opened [PQIndex]
         */
        override fun open(name: Name.IndexName, entity: Entity): PQIndex = PQIndex(name, entity as DefaultEntity)

        /**
         * Initializes the [Store] for a [VAFIndex].
         *
         * @param name The [Name.IndexName] of the [VAFIndex].
         * @param parent The [EntityTx] that requested index initialization.
         * @return True on success, false otherwise.
         */
        override fun initialize(name: Name.IndexName, parent: EntityTx): Boolean = try {
            require(parent is DefaultEntity.Tx) { "PQIndex can only be used with DefaultEntity.Tx" }
            parent.xodusTx.environment.openStore(name.storeName(), StoreConfig.WITHOUT_DUPLICATES, parent.xodusTx, true) != null
        } catch (e:Throwable) {
            LOGGER.error("Failed to initialize PQ index $name due to an exception: ${e.message}.")
            false
        }

        /**
         * De-initializes the [Store] for associated with a [VAFIndex].
         *
         * @param name The [Name.IndexName] of the [LuceneIndex].
         * @param parent The [EntityTx] that requested index de-initialization.
         * @return True on success, false otherwise.
         */
        override fun deinitialize(name: Name.IndexName, parent: EntityTx): Boolean = try {
            require(parent is DefaultEntity.Tx) { "PQIndex can only be used with DefaultEntity.Tx" }
            parent.xodusTx.environment.removeStore(name.storeName(), parent.xodusTx)
            true
        } catch (e:Throwable) {
            LOGGER.error("Failed to de-initialize PQ index $name due to an exception: ${e.message}.")
            false
        }

        /**
         * Generates and returns a [PQIndexConfig] for the given [parameters] (or default values, if [parameters] are not set).
         *
         * @param parameters The parameters to initialize the default [PQIndexConfig] with.
         */
        override fun buildConfig(parameters: Map<String, String>): IndexConfig<PQIndex> = PQIndexConfig(
            parameters[PQIndexConfig.KEY_DISTANCE]?.let { Name.FunctionName.create(it) } ?: EuclideanDistance.FUNCTION_NAME,
            parameters[PQIndexConfig.KEY_NUM_CENTROIDS]?.toInt() ?: PQIndexConfig.DEFAULT_CENTROIDS,
            parameters[PQIndexConfig.KEY_NUM_SUBSPACES]?.toInt() ?: PQIndexConfig.DEFAULT_SUBSPACES,
            parameters[PQIndexConfig.KEY_SEED]?.toInt() ?: System.currentTimeMillis().toInt(),
        )

        /**
         * Returns the [PQIndexConfig.Binding]
         *
         * @return [PQIndexConfig.Binding]
         */
        override fun configBinding(): ComparableBinding = PQIndexConfig.Binding

        /**
         * Trains a new [SingleStageQuantizer] for this [AsyncPQIndexRebuilder].
         *
         * @param indexTx The [IndexTx] to use to train the quantizer.
         * @return A newly trained [SingleStageQuantizer].
         */
        fun trainQuantizer(indexTx: IndexTx): SingleStageQuantizer {
            /* Read basic index properties. */
            val config = indexTx.config as PQIndexConfig
            val column = indexTx.columns[0]

            /* Tx objects required for index rebuilding. */
            val count = indexTx.parent.count()

            /* Generate and obtain signature and distance function. */
            val signature = Signature.Closed(config.distance, arrayOf(indexTx.columns[0].type, indexTx.columns[0].type), Types.Double)
            val distanceFunction: VectorDistance<*> = indexTx.dbo.catalogue.functions.obtain(signature) as VectorDistance<*>

            /* Generates new product quantize. */
            val fraction = ((3.0f * config.numCentroids) / count)
            val seed = System.currentTimeMillis()
            val learningData = DataCollectionUtilities.acquireLearningData(indexTx.parent, column, fraction, seed)
            return SingleStageQuantizer.learnFromData(distanceFunction, learningData, config)
        }
    }

    /** The [IndexType] of this [PQIndex]. */
    override val type: IndexType = IndexType.PQ

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [Index].
     *
     * @param parent If parent [EntityTx] this [IndexTx] belongs to.
     */
    override fun newTx(parent: EntityTx): IndexTx {
        require(parent is DefaultEntity.Tx) { "PQIndex can only be used with DefaultEntity.Tx" }
        return this.transactions.computeIfAbsent(parent.context.txn.transactionId, Long2ObjectFunction {
            val subTransaction = Tx(parent)
            parent.context.txn.registerSubtransaction(subTransaction)
            subTransaction
        })
    }

    /**
     * Opens and returns a new [PQIndexRebuilder] object that can be used to rebuild with this [PQIndex].
     *
     * @param context The [QueryContext] to create [PQIndexRebuilder] for.
     * @return [PQIndexRebuilder]
     */
    override fun newRebuilder(context: QueryContext) = PQIndexRebuilder(this, context)

    /**
     * Opens and returns a new [AsyncPQIndexRebuilder] object that can be used to rebuild with this [PQIndex].
     *
     * @param instance The [Instance] that requested the [AsyncIndexRebuilder].
     * @return [AsyncPQIndexRebuilder]
     */
    override fun newAsyncRebuilder(instance: Instance) = AsyncPQIndexRebuilder(this, instance)

    /**
     * A [IndexTx] that affects this [AbstractIndex].
     */
    inner class Tx(parent: DefaultEntity.Tx) : AbstractIndex.Tx(parent) {

        /** The [VectorDistance] function employed by this [PQIndex]. */
        private val distanceFunction: VectorDistance<*> by lazy {
            val signature = Signature.Closed((this.config as PQIndexConfig).distance, arrayOf(this.columns[0].type, this.columns[0].type), Types.Double)
            this@PQIndex.catalogue.functions.obtain(signature) as VectorDistance<*>
        }

        /** The [SingleStageQuantizer] used by this [PQIndex.Tx] instance. */
        private val quantizer: SingleStageQuantizer by lazy {
            val serializable = IndexStructuralMetadata.read<SerializableSingleStageProductQuantizer>(this, SerializableSingleStageProductQuantizer.Binding)?:
                throw DatabaseException.DataCorruptionException("ProductQuantizer for PQ index ${this@PQIndex.name} is missing.")
            serializable.toProductQuantizer(this.distanceFunction)
        }

        /** The Xodus [Store] used to store [SPQSignature]s. */
        private val dataStore: Store = this.xodusTx.environment.openStore(this@PQIndex.name.storeName(), StoreConfig.USE_EXISTING,  this.xodusTx, false)
            ?: throw DatabaseException.DataCorruptionException("Data store for index ${this@PQIndex.name} is missing.")

        /**
         * Checks if this [PQIndex] can process the provided [Predicate] and returns true if so and false otherwise.
         *
         * @param predicate [Predicate] to check.
         * @return True if [Predicate] can be processed, false otherwise.
         */
        @Synchronized
        override fun canProcess(predicate: Predicate): Boolean {
            if (predicate !is ProximityPredicate.Scan) return false
            if (predicate.column.physical != this.columns[0]) return false
            if (predicate.distance::class != this.distanceFunction::class) return false
            return true
        }

        /**
         * Calculates the count estimate of this [PQIndex.Tx] processing the provided [Predicate].
         *
         * @param predicate [Predicate] to check.
         * @return Count estimate for the [Predicate]
         */

        @Synchronized
        override fun countFor(predicate: Predicate): Long = when (predicate) {
            is ProximityPredicate.Scan -> this.count()
            else -> 0L
        }

        /**
         * Calculates the cost estimate of this [PQIndex.Tx] processing the provided [Predicate].
         *
         * @param predicate [Predicate] to check.
         * @return [Cost] estimate for the [Predicate]
         */
        @Synchronized
        override fun costFor(predicate: Predicate): Cost {
            if (!this.canProcess(predicate)) return Cost.INVALID
            val count = this.count()
            return Cost(
                io = Cost.DISK_ACCESS_READ_SEQUENTIAL.io * Short.SIZE_BYTES,
                cpu = 4 * Cost.MEMORY_ACCESS.cpu + Cost.FLOP.cpu,
                accuracy = 0.2f
            ) * this.quantizer.codebooks.size * count
        }

        /**
         * Returns a [List] of the [ColumnDef] produced by this [PQIndex].
         *
         * @return [List] of [ColumnDef].
         */
        @Synchronized
        override fun columnsFor(predicate: Predicate): List<ColumnDef<*>> {
            require(predicate is ProximityPredicate.Scan) { "PQIndex can only process proximity search." }
            return this.parent.listColumns() + predicate.distanceColumn.column
        }

        /**
         * Returns the map of [Trait]s this [PQIndex] implements for the given [Predicate]s.
         *
         * @param predicate [Predicate] to check.
         * @return Map of [Trait]s for this [PQIndex]
         */
        @Synchronized
        override fun traitsFor(predicate: Predicate): Map<TraitType<*>, Trait> = when (predicate) {
            is ProximityPredicate.Scan -> mutableMapOf()
            else -> throw IllegalArgumentException("Unsupported predicate for high-dimensional index. This is a programmer's error!")
        }

        /**
         * Returns the number of entries in this [VAFIndex].
         *
         * @return Number of entries in this [VAFIndex]
         */
        @Synchronized
        override fun count(): Long  = this.dataStore.count(this.xodusTx)

        /**
         * Tries to apply the change applied by this [DataEvent.Insert] to the [PQIndex] underlying this [PQIndex.Tx]. This method implements the
         * [PQIndex]'es write model: INSERTs are always applied with the existing quantizer.
         *
         * @param event The [DataEvent.Insert] to apply.
         * @return True if change could be applied, false otherwise.
         */
        @Synchronized
        override fun tryApply(event: DataEvent.Insert): Boolean {
            /* Extract value and return true if value is NULL (since NULL values are ignored). */
            val value = event.tuple[this@Tx.columns[0].name] as? RealVectorValue<*> ?: return true
            val sig = this.quantizer.quantize(value)
            return this.dataStore.put(this.xodusTx, event.tuple.tupleId.toKey(), sig.toEntry())
        }

        /**
         * Tries to apply the change applied by this [DataEvent.Update] to the [PQIndex] underlying this [PQIndex.Tx]. This method implements the
         * [PQIndex]'es write model: UPDATES are always applied using the existing quantizer.
         *
         * @param event The [DataEvent.Update] to apply.
         * @return True if change could be applied, false otherwise.
         */
        @Synchronized
        override fun tryApply(event: DataEvent.Update): Boolean {
            val oldValue = event.oldTuple[this.columns[0].name] as? RealVectorValue<*>
            val newValue = event.newTuple[this.columns[0].name] as? RealVectorValue<*>

            /* Obtain marks and update them. */
            return if (newValue is RealVectorValue<*>) { /* Case 1: New value is not null, i.e., update to new value. */
                val newSig = this.quantizer.quantize(newValue as VectorValue<*>)
                this.dataStore.put(this.xodusTx, event.newTuple.tupleId.toKey(), newSig.toEntry())
            } else if (oldValue is RealVectorValue<*>) { /* Case 2: New value is null but old value wasn't, i.e., delete index entry. */
                this.dataStore.delete(this.xodusTx, event.oldTuple.tupleId.toKey())
            } else { /* Case 3: There is no value, there was no value, proceed. */
                true
            }
        }

        /**
         * Tries to apply the change applied by this [DataEvent.Delete] to the [PQIndex] underlying this [PQIndex.Tx]. This method implements the
         * [PQIndex]'es write model: DELETEs are always applied using the existing quantizer.
         *
         * @param event The [DataEvent.Delete] to apply.
         * @return True if change could be applied, false otherwise.
         */
        @Synchronized
        override fun tryApply(event: DataEvent.Delete): Boolean {
            return event.oldTuple[this.columns[0].name] == null || this.dataStore.delete(this.xodusTx, event.oldTuple.tupleId.toKey())
        }

        /**
         * Performs a lookup through this [PQIndex.Tx] and returns a [Iterator] of all [Tuple]s that match the [Predicate].
         * Only supports [ProximityPredicate]s.
         *
         * <strong>Important:</strong> The [Iterator] is not thread safe! It remains to the
         * caller to close the [Iterator]
         *
         * @param predicate The [Predicate] for the lookup
         * @return The resulting [Iterator]
         */
        @Synchronized
        override fun filter(predicate: Predicate): Cursor<Tuple>
            = filter(predicate,this.parent.smallestTupleId() .. this.parent.largestTupleId())

        /**
         * Partitioned filtering is not supported by [PQIndex].
         *
         * @param predicate The [Predicate] for the lookup
         * @param partition The [LongRange] specifying the [TupleId]s that should be considered.
         * @return The resulting [Iterator]
         */
        @Synchronized
        override fun filter(predicate: Predicate, partition: LongRange): Cursor<Tuple> {
            require(predicate is ProximityPredicate.Scan) { "PQIndex can only be used with a SCAN type proximity predicate. This is a programmer's error!" }
            with(MissingTuple) {
                with(this@Tx.context.bindings) {
                    val value = predicate.query.getValue() as? VectorValue<*> ?: throw IllegalArgumentException("PQIndex can only process non-null query values.")
                    return PQIndexCursor(partition, value, this@Tx.quantizer, this@Tx.columnsFor(predicate).toTypedArray(), this@Tx)
                }
            }
        }
    }
}
