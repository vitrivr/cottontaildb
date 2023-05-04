package org.vitrivr.cottontail.dbms.index.pq

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.ShortBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.EuclideanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.queries.nodes.traits.NotPartitionableTrait
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexStructCatalogueEntry
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.index.basic.*
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.AsyncIndexRebuilder
import org.vitrivr.cottontail.dbms.index.basic.rebuilder.IndexRebuilder
import org.vitrivr.cottontail.dbms.index.lucene.LuceneIndex
import org.vitrivr.cottontail.dbms.index.pq.quantizer.MultiStageQuantizer
import org.vitrivr.cottontail.dbms.index.pq.quantizer.SerializableMultiStageProductQuantizer
import org.vitrivr.cottontail.dbms.index.pq.rebuilder.IVFPQIndexRebuilder
import org.vitrivr.cottontail.dbms.index.pq.signature.SPQSignature
import org.vitrivr.cottontail.dbms.index.va.VAFIndex
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import kotlin.concurrent.withLock

/**
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class IVFPQIndex(name: Name.IndexName, parent: DefaultEntity): AbstractIndex(name, parent) {

    /**
     * The [IndexDescriptor] for the [PQIndex].
     */
    companion object: IndexDescriptor<IVFPQIndex> {
        /** [Logger] instance used by [IVFPQIndex]. */
        private val LOGGER: Logger = LoggerFactory.getLogger(IVFPQIndex::class.java)

        /** False since [PQIndex] currently doesn't support incremental updates. */
        override val supportsIncrementalUpdate: Boolean = true

        /** False since [PQIndex] doesn't support asynchronous rebuilds. */
        override val supportsAsyncRebuild: Boolean = false

        /** True since [PQIndex] supports partitioning. */
        override val supportsPartitioning: Boolean = false

        /**
         * Opens a [PQIndex] for the given [Name.IndexName] in the given [DefaultEntity].
         *
         * @param name The [Name.IndexName] of the [PQIndex].
         * @param entity The [Entity] that holds the [PQIndex].
         * @return The opened [PQIndex]
         */
        override fun open(name: Name.IndexName, entity: Entity): IVFPQIndex = IVFPQIndex(name, entity as DefaultEntity)

        /**
         * Initializes the [Store] for a [IVFPQIndex].
         *
         * @param name The [Name.IndexName] of the [IVFPQIndex].
         * @param catalogue [Catalogue] reference.
         * @param context The [TransactionContext] to perform the transaction with.
         * @return True on success, false otherwise.
         */
        override fun initialize(name: Name.IndexName, catalogue: Catalogue, context: TransactionContext): Boolean = try {
            val store = (catalogue as DefaultCatalogue).environment.openStore(name.storeName(), StoreConfig.WITH_DUPLICATES, context.xodusTx, true)
            store != null
        } catch (e:Throwable) {
            LOGGER.error("Failed to initialize IVFPQ index $name due to an exception: ${e.message}.")
            false
        }

        /**
         * De-initializes the [Store] for associated with a [IVFPQIndex].
         *
         * @param name The [Name.IndexName] of the [LuceneIndex].
         * @param catalogue [Catalogue] reference.
         * @param context The [TransactionContext] to perform the transaction with.
         * @return True on success, false otherwise.
         */
        override fun deinitialize(name: Name.IndexName, catalogue: Catalogue, context: TransactionContext): Boolean = try {
            (catalogue as DefaultCatalogue).environment.removeStore(name.storeName(), context.xodusTx)
            true
        } catch (e:Throwable) {
            LOGGER.error("Failed to de-initialize IVFPQ index $name due to an exception: ${e.message}.")
            false
        }

        /**
         * Generates and returns a [IVFPQIndexConfig] for the given [parameters] (or default values, if [parameters] are not set).
         *
         * @param parameters The parameters to initialize the default [IVFPQIndexConfig] with.
         */
        override fun buildConfig(parameters: Map<String, String>): IndexConfig<IVFPQIndex> = IVFPQIndexConfig(
            parameters[IVFPQIndexConfig.KEY_DISTANCE]?.let { Name.FunctionName(it) } ?: EuclideanDistance.FUNCTION_NAME,
            parameters[IVFPQIndexConfig.KEY_NUM_COARSE_CENTROIDS]?.toInt() ?: IVFPQIndexConfig.DEFAULT_COARSE_CENTROIDS,
            parameters[IVFPQIndexConfig.KEY_NUM_CENTROIDS]?.toInt() ?: IVFPQIndexConfig.DEFAULT_CENTROIDS,
            parameters[PQIndexConfig.KEY_NUM_SUBSPACES]?.toInt() ?: IVFPQIndexConfig.DEFAULT_SUBSPACES,
            parameters[IVFPQIndexConfig.KEY_SEED]?.toInt() ?: System.currentTimeMillis().toInt()
        )

        /**
         * Returns the [IVFPQIndexConfig.Binding]
         *
         * @return [IVFPQIndexConfig.Binding]
         */
        override fun configBinding(): ComparableBinding = IVFPQIndexConfig.Binding
    }

    override val type: IndexType = IndexType.IVFPQ
    override fun newTx(context: QueryContext): IndexTx
        = context.txn.getCachedTxForDBO(this) ?: this.Tx(context)

    override fun newRebuilder(context: QueryContext): IndexRebuilder<*> = IVFPQIndexRebuilder(this, context)

    override fun newAsyncRebuilder(context: QueryContext): AsyncIndexRebuilder<*> {
        TODO("Not yet implemented")
    }

    /**
     * A [IndexTx] that affects this [IVFPQIndex].
     */
    inner class Tx(context: QueryContext) : AbstractIndex.Tx(context) {

        /** The [VectorDistance] function employed by this [PQIndex]. */
        internal val distanceFunction: VectorDistance<*> by lazy {
            val signature = Signature.Closed((this.config as IVFPQIndexConfig).distance, arrayOf(this.columns[0].type, this.columns[0].type), Types.Double)
            this@IVFPQIndex.catalogue.functions.obtain(signature) as VectorDistance<*>
        }

        /** The coarse [MultiStageQuantizer] used by this [IVFPQIndex.Tx] instance. */
        internal val quantizer: MultiStageQuantizer by lazy {
            val serializable = IndexStructCatalogueEntry.read<SerializableMultiStageProductQuantizer>(this@IVFPQIndex.name, this@IVFPQIndex.catalogue, this.context.txn.xodusTx, SerializableMultiStageProductQuantizer.Binding)?:
            throw DatabaseException.DataCorruptionException("ProductQuantizer for IVFPQ index ${this@IVFPQIndex.name} is missing.")
            serializable.toProductQuantizer(this.distanceFunction)
        }

        /** The Xodus [Store] used to store [SPQSignature]s. */
        internal val dataStore: Store = this@IVFPQIndex.catalogue.environment.openStore(this@IVFPQIndex.name.storeName(), StoreConfig.USE_EXISTING, this.context.txn.xodusTx, false)
            ?: throw DatabaseException.DataCorruptionException("Data store for IVFPQ index ${this@IVFPQIndex.name} is missing.")

        /**
         * Returns a [List] of the [ColumnDef] produced by this [PQIndex].
         *
         * @return [List] of [ColumnDef].
         */
        override fun columnsFor(predicate: Predicate): List<ColumnDef<*>> = this.txLatch.withLock {
            require(predicate is ProximityPredicate.Scan) { "IVFPQIndex can only process proximity search." }
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
            is ProximityPredicate.Scan -> mapOf(NotPartitionableTrait to NotPartitionableTrait)
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
            if (predicate.distance.name != (this.config as IVFPQIndexConfig).distance) return Cost.INVALID
            val numberOfCoarseCentroids = this.config.numCoarseCentroids
            val nprobe = numberOfCoarseCentroids / 32
            val count = Math.floorDiv(this.count(), numberOfCoarseCentroids) * nprobe
            return Cost(
                io = Cost.DISK_ACCESS_READ.io * Short.SIZE_BYTES,
                cpu = 4 * Cost.MEMORY_ACCESS.cpu + Cost.FLOP.cpu,
                accuracy = 0.1f
            ) * this.quantizer.fine.size * count
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
            val sig = this.quantizer.quantize(event.tupleId, value as VectorValue<*>)
            return this.dataStore.put(this.context.txn.xodusTx, ShortBinding.shortToEntry(sig.first), sig.second.toEntry())
        }

        /**
         * Tries to apply the change applied by this [DataEvent.Update] to the [PQIndex] underlying this [PQIndex.Tx]. This method implements the
         * [PQIndex]'es write model: UPDATES are always applied using the existing quantizer.
         *
         * @param event The [DataEvent.Update] to apply.
         * @return True if change could be applied, false otherwise.
         */
        override fun tryApply(event: DataEvent.Update): Boolean {
            /* Extract value and perform sanity check. */
            val oldValue = event.data[this@Tx.columns[0]]?.first
            val newValue = event.data[this@Tx.columns[0]]?.second

            /* Remove signature to tuple ID mapping. */
            if (oldValue != null) {
                val oldSig = this.quantizer.quantize(event.tupleId, oldValue as VectorValue<*>)
                val cursor = this.dataStore.openCursor(this.context.txn.xodusTx)
                if (cursor.getSearchBoth(ShortBinding.shortToEntry(oldSig.first), oldSig.second.toEntry())) {
                    cursor.deleteCurrent()
                }
                cursor.close()
            }

            /* Generate signature and store it. */
            if (newValue != null) {
                val newSig = this.quantizer.quantize(event.tupleId, newValue as VectorValue<*>)
                return this.dataStore.put(this.context.txn.xodusTx, ShortBinding.shortToEntry(newSig.first), newSig.second.toEntry())
            }
            return true
        }

        /**
         * Tries to apply the change applied by this [DataEvent.Delete] to the [PQIndex] underlying this [PQIndex.Tx]. This method implements the
         * [PQIndex]'es write model: DELETEs are always applied using the existing quantizer.
         *
         * @param event The [DataEvent.Delete] to apply.
         * @return True if change could be applied, false otherwise.
         */
        override fun tryApply(event: DataEvent.Delete): Boolean {
            val oldValue = event.data[this.columns[0]] ?: return true
            val sig = this.quantizer.quantize(event.tupleId, oldValue as VectorValue<*>)
            val cursor = this.dataStore.openCursor(this.context.txn.xodusTx)
            if (cursor.getSearchBoth(ShortBinding.shortToEntry(sig.first), sig.second.toEntry())) {
                cursor.deleteCurrent()
            }
            cursor.close()
            return true
        }

        /**
         * Performs a lookup through this [IVFPQIndex.Tx] and returns a [IVFPQIndexCursor] of all [Record]s that match the [Predicate].
         * Only supports [ProximityPredicate.Scan]s.
         *
         * <strong>Important:</strong> The [IVFPQIndexCursor] is not thread safe! It remains to the caller to close the [IVFPQIndexCursor]
         *
         * @param predicate The [Predicate] for the lookup
         * @return The resulting [IVFPQIndexCursor]
         */
        override fun filter(predicate: Predicate): Cursor<Record> = this.txLatch.withLock {
            require(predicate is ProximityPredicate.Scan) { "IVFPQIndex can only be used with a SCAN type proximity predicate. This is a programmer's error!" }
            IVFPQIndexCursor(predicate, this)
        }

        /**
         * Partitioned filtering is not supported by [IVFPQIndex].
         *
         * @param predicate The [Predicate] for the lookup
         * @param partition The [LongRange] specifying the [TupleId]s that should be considered.
         * @return The resulting [Iterator]
         */
        override fun filter(predicate: Predicate, partition: LongRange): Cursor<Record>
            = throw UnsupportedOperationException("IVFPQIndex does not support range partitioning.")
    }
}