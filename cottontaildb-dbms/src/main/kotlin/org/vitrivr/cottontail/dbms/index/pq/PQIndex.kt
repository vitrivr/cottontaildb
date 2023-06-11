package org.vitrivr.cottontail.dbms.index.pq

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.EuclideanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.RealVectorValue
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexStructCatalogueEntry
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.index.basic.*
import org.vitrivr.cottontail.dbms.index.lucene.LuceneIndex
import org.vitrivr.cottontail.dbms.index.pq.quantizer.SerializableSingleStageProductQuantizer
import org.vitrivr.cottontail.dbms.index.pq.quantizer.SingleStageQuantizer
import org.vitrivr.cottontail.dbms.index.pq.rebuilder.AsyncPQIndexRebuilder
import org.vitrivr.cottontail.dbms.index.pq.rebuilder.PQIndexRebuilder
import org.vitrivr.cottontail.dbms.index.pq.signature.SPQSignature
import org.vitrivr.cottontail.dbms.index.va.VAFIndex
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import kotlin.concurrent.withLock

/**
 * An [AbstractIndex] structure for proximity based queries that uses the product quantization (PQ) algorithm described in.
 *
 *  Can be used for all type of [VectorValue]s and distance metrics. Implements the ADC algorithm described in [1], which is well
 * suited for million-scale datasets.
 *
 * References:
 * [1] Jegou, Herve, et al. "Product Quantization for Nearest Neighbor Search." IEEE Transactions on Pattern Analysis and Machine Intelligence. 2010.
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 3.5.0
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
         * @param catalogue [Catalogue] reference.
         * @param context The [TransactionContext] to perform the transaction with.
         * @return True on success, false otherwise.
         */
        override fun initialize(name: Name.IndexName, catalogue: Catalogue, context: TransactionContext): Boolean = try {
            val store = catalogue.transactionManager.environment.openStore(name.storeName(), StoreConfig.WITHOUT_DUPLICATES, context.xodusTx, true)
            store != null
        } catch (e:Throwable) {
            LOGGER.error("Failed to initialize PQ index $name due to an exception: ${e.message}.")
            false
        }

        /**
         * De-initializes the [Store] for associated with a [VAFIndex].
         *
         * @param name The [Name.IndexName] of the [LuceneIndex].
         * @param catalogue [Catalogue] reference.
         * @param context The [TransactionContext] to perform the transaction with.
         * @return True on success, false otherwise.
         */
        override fun deinitialize(name: Name.IndexName, catalogue: Catalogue, context: TransactionContext): Boolean = try {
            catalogue.transactionManager.environment.removeStore(name.storeName(), context.xodusTx)
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
            parameters[PQIndexConfig.KEY_DISTANCE]?.let { Name.FunctionName(it) } ?: EuclideanDistance.FUNCTION_NAME,
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
    }

    /** The [IndexType] of this [PQIndex]. */
    override val type: IndexType = IndexType.PQ

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [PQIndex].
     *
     * @param context The [QueryContext] to create this [IndexTx] for.
     * @return [Tx]
     */
    override fun newTx(context: QueryContext): IndexTx
        = context.txn.getCachedTxForDBO(this) ?: this.Tx(context)

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
     * @param context If the [QueryContext] that requested the [AsyncPQIndexRebuilder].
     * @return [AsyncPQIndexRebuilder]
     */
    override fun newAsyncRebuilder(context: QueryContext) = AsyncPQIndexRebuilder(this, context)

    /**
     * A [IndexTx] that affects this [AbstractIndex].
     */
    inner class Tx(context: QueryContext) : AbstractIndex.Tx(context) {

        /** The [VectorDistance] function employed by this [PQIndex]. */
        internal val distanceFunction: VectorDistance<*> by lazy {
            val signature = Signature.Closed((this.config as PQIndexConfig).distance, arrayOf(this.columns[0].type, this.columns[0].type), Types.Double)
            this@PQIndex.catalogue.functions.obtain(signature) as VectorDistance<*>
        }

        /** The [SingleStageQuantizer] used by this [PQIndex.Tx] instance. */
        internal val quantizer: SingleStageQuantizer by lazy {
            val serializable = IndexStructCatalogueEntry.read<SerializableSingleStageProductQuantizer>(this@PQIndex.name, this@PQIndex.catalogue, this.context.txn.xodusTx, SerializableSingleStageProductQuantizer.Binding)?:
                throw DatabaseException.DataCorruptionException("ProductQuantizer for PQ index ${this@PQIndex.name} is missing.")
            serializable.toProductQuantizer(this.distanceFunction)
        }

        /** The Xodus [Store] used to store [SPQSignature]s. */
        internal val dataStore: Store = this@PQIndex.catalogue.transactionManager.environment.openStore(this@PQIndex.name.storeName(), StoreConfig.USE_EXISTING, this.context.txn.xodusTx, false)
            ?: throw DatabaseException.DataCorruptionException("Data store for index ${this@PQIndex.name} is missing.")

        /**
         * Returns a [List] of the [ColumnDef] produced by this [PQIndex].
         *
         * @return [List] of [ColumnDef].
         */
        override fun columnsFor(predicate: Predicate): List<ColumnDef<*>> = this.txLatch.withLock {
            require(predicate is ProximityPredicate.Scan) { "PQIndex can only process proximity search." }
            return listOf(predicate.distanceColumn)
        }

        /**
         * Checks if this [PQIndex] can process the provided [Predicate] and returns true if so and false otherwise.
         *
         * @param predicate [Predicate] to check.
         * @return True if [Predicate] can be processed, false otherwise.
         */
        override fun canProcess(predicate: Predicate): Boolean
            = predicate is ProximityPredicate.Scan && predicate.column == this.columns[0] && predicate.distance::class == this.distanceFunction::class

        /**
         * Returns the map of [Trait]s this [PQIndex] implements for the given [Predicate]s.
         *
         * @param predicate [Predicate] to check.
         * @return Map of [Trait]s for this [PQIndex]
         */
        override fun traitsFor(predicate: Predicate): Map<TraitType<*>, Trait> = when (predicate) {
            is ProximityPredicate.Scan -> mutableMapOf()
            else -> throw IllegalArgumentException("Unsupported predicate for high-dimensional index. This is a programmer's error!")
        }

        /**
         * Calculates the cost estimate of this [PQIndex.Tx] processing the provided [Predicate].
         *
         * @param predicate [Predicate] to check.
         * @return [Cost] estimate for the [Predicate]
         */
        override fun costFor(predicate: Predicate): Cost = this.txLatch.withLock {
            if (predicate !is ProximityPredicate.Scan) return Cost.INVALID
            if (predicate.column != this.columns[0]) return Cost.INVALID
            if (predicate.distance.name != (this.config as PQIndexConfig).distance) return Cost.INVALID
            val count = this.count()
            return Cost(
                io = Cost.DISK_ACCESS_READ.io * Short.SIZE_BYTES,
                cpu = 4 * Cost.MEMORY_ACCESS.cpu + Cost.FLOP.cpu,
                accuracy = 0.2f
            ) * this.quantizer.codebooks.size * count
        }

        /**
         * Returns the number of entries in this [VAFIndex].
         *
         * @return Number of entries in this [VAFIndex]
         */
        override fun count(): Long  = this.txLatch.withLock {
            this.dataStore.count(this.context.txn.xodusTx)
        }

        /**
         * Tries to apply the change applied by this [DataEvent.Insert] to the [PQIndex] underlying this [PQIndex.Tx]. This method implements the
         * [PQIndex]'es write model: INSERTs are always applied with the existing quantizer.
         *
         * @param event The [DataEvent.Insert] to apply.
         * @return True if change could be applied, false otherwise.
         */
        override fun tryApply(event: DataEvent.Insert): Boolean {
            /* Extract value and return true if value is NULL (since NULL values are ignored). */
            val value = event.data[this@Tx.columns[0]] ?: return true
            val sig = this.quantizer.quantize(value as RealVectorValue<*>)
            return this.dataStore.put(this.context.txn.xodusTx, event.tupleId.toKey(), sig.toEntry())
        }

        /**
         * Tries to apply the change applied by this [DataEvent.Update] to the [PQIndex] underlying this [PQIndex.Tx]. This method implements the
         * [PQIndex]'es write model: UPDATES are always applied using the existing quantizer.
         *
         * @param event The [DataEvent.Update] to apply.
         * @return True if change could be applied, false otherwise.
         */
        override fun tryApply(event: DataEvent.Update): Boolean {
            val oldValue = event.data[this.columns[0]]?.first
            val newValue = event.data[this.columns[0]]?.second

            /* Obtain marks and update them. */
            return if (newValue is RealVectorValue<*>) { /* Case 1: New value is not null, i.e., update to new value. */
                val newSig = this.quantizer.quantize(newValue as VectorValue<*>)
                this.dataStore.put(this.context.txn.xodusTx, event.tupleId.toKey(), newSig.toEntry())
            } else if (oldValue is RealVectorValue<*>) { /* Case 2: New value is null but old value wasn't, i.e., delete index entry. */
                this.dataStore.delete(this.context.txn.xodusTx, event.tupleId.toKey())
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
        override fun tryApply(event: DataEvent.Delete): Boolean {
            return event.data[this.columns[0]] == null || this.dataStore.delete(this.context.txn.xodusTx, event.tupleId.toKey())
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
        @Suppress("UNCHECKED_CAST")
        override fun filter(predicate: Predicate): Cursor<Tuple> = this.txLatch.withLock {
            val entityTx = this@PQIndex.parent.newTx(this.context)
            filter(predicate,entityTx.smallestTupleId() .. entityTx.largestTupleId())
        }

        /**
         * Partitioned filtering is not supported by [PQIndex].
         *
         * @param predicate The [Predicate] for the lookup
         * @param partition The [LongRange] specifying the [TupleId]s that should be considered.
         * @return The resulting [Iterator]
         */
        override fun filter(predicate: Predicate, partition: LongRange): Cursor<Tuple> = this.txLatch.withLock {
            require(predicate is ProximityPredicate.Scan) { "PQIndex can only be used with a SCAN type proximity predicate. This is a programmer's error!" }
            PQIndexCursor(partition, predicate, this)
        }
    }
}
