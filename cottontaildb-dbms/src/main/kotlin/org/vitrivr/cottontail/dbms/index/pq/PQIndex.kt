package org.vitrivr.cottontail.dbms.index.pq

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.LongBinding
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
import org.vitrivr.cottontail.core.queries.nodes.traits.Trait
import org.vitrivr.cottontail.core.queries.nodes.traits.TraitType
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.types.RealVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.VectorValue
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexStructCatalogueEntry
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.index.basic.*
import org.vitrivr.cottontail.dbms.index.lucene.LuceneIndex
import org.vitrivr.cottontail.dbms.index.pq.rebuilder.AsyncPQIndexRebuilder
import org.vitrivr.cottontail.dbms.index.pq.rebuilder.PQIndexRebuilder
import org.vitrivr.cottontail.dbms.index.pq.signature.PQLookupTable
import org.vitrivr.cottontail.dbms.index.pq.signature.PQSignature
import org.vitrivr.cottontail.dbms.index.pq.signature.ProductQuantizer
import org.vitrivr.cottontail.dbms.index.pq.signature.SerializableProductQuantizer
import org.vitrivr.cottontail.dbms.index.va.VAFIndex
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.withLock

/**
 * An [AbstractIndex] structure for proximity based queries that uses a product quantization (PQ).
 * Can be used for all type of [VectorValue]s and distance metrics.
 *
 * References:
 * [1] Guo, Ruiqi, et al. "Quantization based fast inner product search." Artificial Intelligence and Statistics. 2016.
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 3.4.0
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
            val store = (catalogue as DefaultCatalogue).environment.openStore(name.storeName(), StoreConfig.WITHOUT_DUPLICATES, context.xodusTx, true)
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
            (catalogue as DefaultCatalogue).environment.removeStore(name.storeName(), context.xodusTx)
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
     * @param context The [TransactionContext] to create this [IndexTx] for.
     * @return [Tx]
     */
    override fun newTx(context: TransactionContext): IndexTx = Tx(context)

    /**
     * Opens and returns a new [PQIndexRebuilder] object that can be used to rebuild with this [PQIndex].
     *
     * @param context The [TransactionContext] to create [PQIndexRebuilder] for.
     * @return [PQIndexRebuilder]
     */
    override fun newRebuilder(context: TransactionContext) = PQIndexRebuilder(this, context)

    /**
     * Opens and returns a new [AsyncPQIndexRebuilder] object that can be used to rebuild with this [PQIndex].
     *
     * @return [AsyncPQIndexRebuilder]
     */
    override fun newAsyncRebuilder() = AsyncPQIndexRebuilder(this)

    /**
     * A [IndexTx] that affects this [AbstractIndex].
     */
    private inner class Tx(context: TransactionContext) : AbstractIndex.Tx(context) {

        /** The [VectorDistance] function employed by this [PQIndex]. */
        private val distanceFunction: VectorDistance<*> by lazy {
            val signature = Signature.Closed((this.config as PQIndexConfig).distance, arrayOf(this.columns[0].type, this.columns[0].type), Types.Double)
            this@PQIndex.catalogue.functions.obtain(signature) as VectorDistance<*>
        }

        /** The [ProductQuantizer] used by this [PQIndex.Tx] instance. */
        private val quantizer: ProductQuantizer by lazy {
            val serializable = IndexStructCatalogueEntry.read<SerializableProductQuantizer>(this@PQIndex.name, this@PQIndex.catalogue, context.xodusTx, SerializableProductQuantizer.Binding)?:
                throw DatabaseException.DataCorruptionException("ProductQuantizer for PQ index ${this@PQIndex.name} is missing.")
            serializable.toProductQuantizer(this.distanceFunction)
        }

        /** The Xodus [Store] used to store [PQSignature]s. */
        private val dataStore: Store = this@PQIndex.catalogue.environment.openStore(this@PQIndex.name.storeName(), StoreConfig.USE_EXISTING, this.context.xodusTx, false)
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
            return Cost(count * this.config.subspaces * Cost.DISK_ACCESS_READ.io, count * (4 * Cost.MEMORY_ACCESS.cpu + Cost.FLOP.cpu) + predicate.cost.cpu)
        }

        /**
         * Returns the number of entries in this [VAFIndex].
         *
         * @return Number of entries in this [VAFIndex]
         */
        override fun count(): Long  = this.txLatch.withLock {
            this.dataStore.count(this.context.xodusTx)
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
            return this.dataStore.put(this.context.xodusTx, PQSignature.Binding.valueToEntry(sig), event.tupleId.toKey())
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
                val oldSig = this.quantizer.quantize(oldValue as VectorValue<*>)
                val cursor = this.dataStore.openCursor(this.context.xodusTx)
                if (cursor.getSearchBoth(PQSignature.Binding.valueToEntry(oldSig), event.tupleId.toKey())) {
                    cursor.deleteCurrent()
                }
                cursor.close()
            }

            /* Generate signature and store it. */
            if (newValue != null) {
                val newSig = this.quantizer.quantize(newValue as VectorValue<*>)
                return this.dataStore.put(this.context.xodusTx, PQSignature.Binding.valueToEntry(newSig), event.tupleId.toKey())
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
            val sig = this.quantizer.quantize(oldValue as VectorValue<*>)
            val cursor = this.dataStore.openCursor(this.context.xodusTx)
            if (cursor.getSearchBoth(PQSignature.Binding.valueToEntry(sig), event.tupleId.toKey())) {
                cursor.deleteCurrent()
            }
            cursor.close()
            return true
        }

        /**
         * Performs a lookup through this [PQIndex.Tx] and returns a [Iterator] of all [Record]s that match the [Predicate].
         * Only supports [ProximityPredicate]s.
         *
         * <strong>Important:</strong> The [Iterator] is not thread safe! It remains to the
         * caller to close the [Iterator]
         *
         * @param predicate The [Predicate] for the lookup
         * @return The resulting [Iterator]
         */
        @Suppress("UNCHECKED_CAST")
        override fun filter(predicate: Predicate): Cursor<Record> = this.txLatch.withLock {
            val entityTx = this.context.getTx(this@PQIndex.parent) as EntityTx
            filter(predicate,entityTx.smallestTupleId() .. entityTx.largestTupleId())
        }

        /**
         * Partitioned filtering is not supported by [PQIndex].
         *
         * @param predicate The [Predicate] for the lookup
         * @param partition The [LongRange] specifying the [TupleId]s that should be considered.
         * @return The resulting [Iterator]
         */
        override fun filter(predicate: Predicate, partition: LongRange): Cursor<Record> = this.txLatch.withLock {
            object : Cursor<Record> {
                /** Cast to [ProximityPredicate] (if such a cast is possible).  */
                private val predicate = if (predicate is ProximityPredicate) {
                    predicate
                } else {
                    throw QueryException.UnsupportedPredicateException("Index '${this@PQIndex.name}' (PQ Index) does not support predicates of type '${predicate::class.simpleName}'.")
                }

                /** Prepares [PQLookupTable]s for the given query vector(s). */
                private val lookupTable: PQLookupTable = this@Tx.quantizer.createLookupTable(this.predicate.query.value as VectorValue<*>)

                /** The sub-transaction this [Cursor] operates upon.  */
                private val subTx = this@Tx.context.xodusTx.readonlySnapshot

                /** The internal cursor used by this index. */
                private val cursor = this@Tx.dataStore.openCursor(this@Tx.context.xodusTx)

                /** The start key. */
                private val startKey = partition.first.toKey()

                /* The end key. */
                private val endKey = partition.last.toKey()

                /** The [ColumnDef] produced by  this [Cursor]. */
                private val produces = this@Tx.columnsFor(predicate).toTypedArray()

                /** A begin of cursor flag. */
                private var boc = AtomicBoolean(true)

                init {
                    if (this.cursor.getSearchKeyRange(this.startKey) == null) {
                        this.boc.set(false)
                    }
                }

                /**
                 * Moves the internal cursor and return true, as long as new candidates appear.
                 */
                override fun moveNext(): Boolean
                    = (this.boc.compareAndExchange(true, false) || (this.cursor.next && this.cursor.key <= this.endKey))

                /**
                 * Returns the current [TupleId] this [Cursor] is pointing to.
                 *
                 * @return [TupleId]
                 */
                override fun key(): TupleId = LongBinding.compressedEntryToLong(this.cursor.key)

                /**
                 * Returns the current [Record] this [Cursor] is pointing to.
                 *
                 * @return [TupleId]
                 */
                override fun value(): Record {
                    val signature = PQSignature.Binding.entryToValue(cursor.value)
                    val approximation = DoubleValue(this.lookupTable.approximateDistance(signature))
                    return StandaloneRecord(LongBinding.compressedEntryToLong(cursor.key), this.produces, arrayOf(approximation))
                }

                /**
                 *
                 */
                override fun close() {
                    this.subTx.abort()
                    this.cursor.close()
                }
            }
        }
    }
}
