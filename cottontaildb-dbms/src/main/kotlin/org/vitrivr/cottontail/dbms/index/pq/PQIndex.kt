package org.vitrivr.cottontail.dbms.index.pq

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.apache.commons.math3.random.JDKRandomGenerator
import org.apache.commons.math3.random.RandomGenerator
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.functions.Argument
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.EuclideanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.VectorDistance
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.types.RealVectorValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.VectorValue
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.column.ColumnTx
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.events.DataEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.execution.operators.sort.RecordComparator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.index.*
import org.vitrivr.cottontail.dbms.index.lucene.LuceneIndex
import org.vitrivr.cottontail.dbms.index.va.VAFIndex
import org.vitrivr.cottontail.utilities.selection.HeapSelection
import java.util.*
import kotlin.concurrent.withLock

/**
 * An [AbstractHDIndex] structure for proximity based queries that uses a product quantization (PQ).
 * Can be used for all type of [VectorValue]s and distance metrics.
 *
 * References:
 * [1] Guo, Ruiqi, et al. "Quantization based fast inner product search." Artificial Intelligence and Statistics. 2016.
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 3.0.0
 */
class PQIndex(name: Name.IndexName, parent: DefaultEntity) : AbstractHDIndex(name, parent) {

    /**
     * The [IndexDescriptor] for the [PQIndex].
     */
    companion object: IndexDescriptor<PQIndex> {
        /** [Logger] instance used by [PQIndex]. */
        private val LOGGER: Logger = LoggerFactory.getLogger(PQIndex::class.java)

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
         * @param entity The [DefaultEntity] that holds the [PQIndex].
         * @return The opened [PQIndex]
         */
        override fun open(name: Name.IndexName, entity: DefaultEntity): PQIndex = PQIndex(name, entity)

        /**
         * Initializes the [Store] for a [VAFIndex].
         *
         * @param name The [Name.IndexName] of the [VAFIndex].
         * @param entity The [DefaultEntity] that executes the operation.
         * @return True on success, false otherwise.
         */
        override fun initialize(name: Name.IndexName, entity: DefaultEntity.Tx): Boolean = try {
            val store = entity.dbo.catalogue.environment.openStore(name.storeName(), StoreConfig.WITH_DUPLICATES, entity.context.xodusTx, true)
            store != null
        } catch (e:Throwable) {
            LOGGER.error("Failed to initialize PQ index $name due to an exception: ${e.message}.")
            false
        }

        /**
         * De-initializes the [Store] for associated with a [VAFIndex].
         *
         * @param name The [Name.IndexName] of the [LuceneIndex].
         * @param entity The [DefaultEntity.Tx] that executes the operation.
         * @return True on success, false otherwise.
         */
        override fun deinitialize(name: Name.IndexName, entity: DefaultEntity.Tx): Boolean = try {
            entity.dbo.catalogue.environment.removeStore(name.storeName(), entity.context.xodusTx)
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
            parameters[PQIndexConfig.KEY_SAMPLE_SIZE]?.toInt() ?: 3000,
            parameters[PQIndexConfig.KEY_NUM_CENTROIDS]?.toInt() ?: PQIndexConfig.DEFAULT_CENTROIDS
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
     */
    override fun newTx(context: TransactionContext): IndexTx = Tx(context)

    /**
     * Closes this [PQIndex]
     */
    override fun close() {
        /* No op. */
    }

    /**
     * A [IndexTx] that affects this [AbstractIndex].
     */
    private inner class Tx(context: TransactionContext) : AbstractHDIndex.Tx(context) {

        /** The [PQIndexConfig] used by this [PQIndex] instance. */
        override val config: PQIndexConfig
            get() = super.config as PQIndexConfig

        /** The set of supported [VectorDistance]s. */
        override val supportedDistances: Set<Signature.Closed<*>>

        /** The [VectorDistance] function employed by this [PQIndex]. */
        private val distanceFunction: VectorDistance<*> = this.config.let {
            val signature = Signature.Closed(config.distance, arrayOf(this.column.type, this.column.type), Types.Double)
            this.supportedDistances = setOf(Signature.Closed(config.distance, arrayOf(this.column.type, this.column.type), Types.Double))
            this@PQIndex.catalogue.functions.obtain(signature) as VectorDistance<*>
        }

        /** The Xodus [Store] used to store [PQSignature]s. */
        private var dataStore: Store = this@PQIndex.catalogue.environment.openStore(this@PQIndex.name.storeName(), StoreConfig.USE_EXISTING, this.context.xodusTx, false)
            ?: throw DatabaseException.DataCorruptionException("Data store for index ${this@PQIndex.name} is missing.")

        /** The [ProductQuantizer] used by this [PQIndex.Tx] instance. */
        private var quantizer = ProductQuantizer.loadFromConfig(this.distanceFunction, this.config)

        /**
         * Returns a [List] of the [ColumnDef] produced by this [PQIndex].
         *
         * @return [List] of [ColumnDef].
         */
        override fun columnsFor(predicate: Predicate): List<ColumnDef<*>> = this.txLatch.withLock {
            require(predicate is ProximityPredicate) { "PQIndex can only process proximity predicates." }
            return listOf(predicate.distanceColumn)
        }

        /**
         * Calculates the cost estimate of this [PQIndex.Tx] processing the provided [Predicate].
         *
         * @param predicate [Predicate] to check.
         * @return [Cost] estimate for the [Predicate]
         */
        override fun costFor(predicate: Predicate): Cost = this.txLatch.withLock {
            if (predicate !is ProximityPredicate) return Cost.INVALID
            if (predicate.column != this.columns[0]) return Cost.INVALID
            if (predicate.distance.name != this.config.distance) return Cost.INVALID
            val count = this.count()
            val subspaces = ProductQuantizer.numberOfSubspaces(predicate.column.type.logicalSize)
            return Cost(
                count * subspaces * Cost.DISK_ACCESS_READ.io + predicate.k * predicate.column.type.logicalSize * Cost.DISK_ACCESS_READ.io,
                count * (4 * Cost.MEMORY_ACCESS.cpu + Cost.FLOP.cpu) + predicate.cost.cpu * predicate.k,
                (predicate.k * this.columnsFor(predicate).sumOf { it.type.physicalSize }).toFloat()
            )
        }

        /**
         * (Re-)builds the [PQIndex] from scratch.
         */
        override fun rebuild() = this.txLatch.withLock {
            /* Obtain some learning data for training. */
            LOGGER.debug("Rebuilding PQ index {}", this@PQIndex.name)
            val random = JDKRandomGenerator(System.currentTimeMillis().toInt())
            val entityTx = this.context.getTx(this.dbo.parent) as EntityTx

            /* Obtain PQ data structure. */
            val config = this.config
            val column = this.columns[0]
            val type = column.type as Types.Vector<*,*>
            val columnTx = this.context.getTx(entityTx.columnForName(this.columns[0].name)) as ColumnTx<*>
            val distanceFunction = this@PQIndex.catalogue.functions.obtain(Signature.SemiClosed(config.distance, arrayOf(Argument.Typed(type), Argument.Typed(type)))) as VectorDistance<*>
            val quantizer = ProductQuantizer.learnFromData(distanceFunction, this.acquireLearningData(columnTx, random), config)

            /* Clear old signatures. */
            this.clear()

            /* Iterate over column and update index with entries. */
            val cursor = columnTx.cursor()
            while (cursor.moveNext()) {
                val value = cursor.value()
                if (value is VectorValue<*>) {
                    val sig = quantizer.quantize(value)
                    this.dataStore.put(this.context.xodusTx, PQSignature.Binding.valueToEntry(sig), cursor.key().toKey())
                }
            }

            /* Close cursor. */
            cursor.close()

            /* Update index state for index. */
            this.quantizer = quantizer
            this.updateState(IndexState.CLEAN, this.config.copy(centroids = quantizer.centroids()))
            LOGGER.debug("Rebuilding PQ index {} completed!", this@PQIndex.name)
        }

        /**
         * Always throws an [UnsupportedOperationException], since [PQIndex] does not support asynchronous rebuilds.
         */
        override fun asyncRebuild() = throw UnsupportedOperationException("PQIndex does not support asynchronous rebuild.")

        /**
         * Returns the number of entries in this [VAFIndex].
         *
         * @return Number of entries in this [VAFIndex]
         */
        override fun count(): Long  = this.txLatch.withLock {
            this.dataStore.count(this.context.xodusTx)
        }

        /**
         * Clears the [VAFIndex] underlying this [Tx] and removes all entries it contains.
         */
        override fun clear() = this.txLatch.withLock {
            /* Truncate and replace store.*/
            this@PQIndex.catalogue.environment.truncateStore(this@PQIndex.name.storeName(), this.context.xodusTx)
            this.dataStore = this@PQIndex.catalogue.environment.openStore(this@PQIndex.name.storeName(), StoreConfig.USE_EXISTING, this.context.xodusTx, false)
                ?: throw DatabaseException.DataCorruptionException("Data store for index ${this@PQIndex.name} is missing.")

            /* Update catalogue entry for index. */
            this.updateState(IndexState.STALE)
        }

        /**
         * Tries to apply the change applied by this [DataEvent.Insert] to the [PQIndex] underlying this [PQIndex.Tx]. This method implements the
         * [PQIndex]'es write model: INSERTs are always applied with the existing quantizer.
         *
         * @param event The [DataEvent.Insert] to apply.
         * @return True if change could be applied, false otherwise.
         */
        override fun tryApply(event: DataEvent.Insert): Boolean {
            /* Extract value and perform sanity check. */
            val value = event.data[this@Tx.column]
            require(value is RealVectorValue<*>) { "Only real vector values can be stored in a VAFIndex. This is a programmer's error!" }

            /* Generate signature and store it. */
            val sig = this.quantizer.quantize(value)
            return this.dataStore.put(this.context.xodusTx, PQSignature.Binding.valueToEntry(sig), event.tupleId.toKey())
        }

        /**
         * Tries to apply the change applied by this [DataEvent.Update] to the [PQIndex] underlying this [PQIndex.Tx]. This method implements the
         * [PQIndex]'es write model: UPDATEs are always applied using the existing quantizer.
         *
         * @param event The [DataEvent.Update] to apply.
         * @return True if change could be applied, false otherwise.
         */
        override fun tryApply(event: DataEvent.Update): Boolean {
            /* Extract value and perform sanity check. */
            val oldValue = event.data[this@Tx.column]?.first
            val newValue = event.data[this@Tx.column]?.second

            /* Remove signature to tuple ID mapping. */
            if (oldValue is VectorValue<*>) {
                val oldSig = this.quantizer.quantize(oldValue)
                val cursor = this.dataStore.openCursor(this.context.xodusTx)
                if (cursor.getSearchBoth(PQSignature.Binding.valueToEntry(oldSig), event.tupleId.toKey())) {
                    cursor.deleteCurrent()
                }
                cursor.close()
            }

            /* Generate signature and store it. */
            if (newValue is VectorValue<*>) {
                val newSig = this.quantizer.quantize(newValue)
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
            val sig = this.quantizer.quantize(event.data[this@Tx.column]!! as VectorValue<*>)
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
        override fun filter(predicate: Predicate): Cursor<Record> = this.txLatch.withLock {
            object : Cursor<Record> {
                /** Local [PQIndexConfig] instance. */
                private val config = this@Tx.config

                /** Cast to [ProximityPredicate] (if such a cast is possible).  */
                private val predicate = if (predicate is ProximityPredicate) {
                    predicate
                } else {
                    throw QueryException.UnsupportedPredicateException("Index '${this@PQIndex.name}' (PQ Index) does not support predicates of type '${predicate::class.simpleName}'.")
                }

                /** Internal [ColumnTx] used to access actual values. */
                private val columnTx: ColumnTx<RealVectorValue<*>>

                /** Prepares [PQLookupTable]s for the given query vector(s). */
                private val lookupTable: PQLookupTable = this@Tx.quantizer.createLookupTable(this.predicate.query.value as VectorValue<*>)

                /** The [HeapSelection] use for finding the top k entries. */
                private var selection = when(this.predicate) {
                    is ProximityPredicate.NNS -> HeapSelection(this.predicate.k, RecordComparator.SingleNonNullColumnComparator(this.predicate.distanceColumn, SortOrder.ASCENDING))
                    is ProximityPredicate.FNS -> HeapSelection(this.predicate.k, RecordComparator.SingleNonNullColumnComparator(this.predicate.distanceColumn, SortOrder.DESCENDING))
                }

                /** The current [Cursor] position. */
                private var position = -1L

                init {
                    /* Obtain Tx object for column. */
                    val entityTx: EntityTx = this@Tx.context.getTx(this@PQIndex.parent) as EntityTx
                    this.columnTx = this@Tx.context.getTx(entityTx.columnForName(this@Tx.columns[0].name)) as ColumnTx<RealVectorValue<*>>
                }

                /**
                 * Moves the internal cursor and return true, as long as new candidates appear.
                 */
                override fun moveNext(): Boolean {
                    if (this.selection.added == 0L) this.prepare()
                    return (++this.position) < this.selection.size
                }

                /**
                 * Returns the current [TupleId] this [Cursor] is pointing to.
                 *
                 * @return [TupleId]
                 */
                override fun key(): TupleId = this.selection[this.position].tupleId

                /**
                 * Returns the current [Record] this [Cursor] is pointing to.
                 *
                 * @return [TupleId]
                 */
                override fun value(): Record = this.selection[this.position]

                /**
                 *
                 */
                override fun close() { }

                /**
                 * Executes the kNN and prepares the results to return by this [Iterator].
                 */
                private fun prepare() {
                    /* Prepare data structures for NNS. */
                    val comparator = when (this.predicate) {
                        is ProximityPredicate.NNS -> Comparator<Pair<PQSignature, Double>> { o1, o2 -> o1.second.compareTo(o2.second) }
                        is ProximityPredicate.FNS -> Comparator<Pair<PQSignature, Double>> { o1, o2 -> -o1.second.compareTo(o2.second) }
                    }

                    /* Phase 1: Perform pre-NNS based on signatures. */
                    val preSelection = HeapSelection(this.predicate.k, comparator)
                    val produces = this@Tx.columnsFor(predicate).toTypedArray()
                    this@Tx.dataStore.openCursor(this@Tx.context.xodusTx).use { cursor ->
                        var scan = 0L
                        while (cursor.nextNoDup) {
                            val signature = PQSignature.Binding.entryToValue(cursor.key)
                            val approximation = this.lookupTable.approximateDistance(signature)
                            preSelection.offer(signature to approximation)
                            scan++
                        }
                        LOGGER.debug("PQ scan: Read $scan signatures and considered ${preSelection.k} of them.")
                    }

                    /* Phase 2: Perform exact NNS based on pre-NNS results. */
                    this@Tx.dataStore.openCursor(this@Tx.context.xodusTx).use { cursor ->
                        val query = this.predicate.query.value as VectorValue<*>
                        for (j in 0 until preSelection.size) {
                            val signature = preSelection[j].first
                            if (cursor.getSearchKey(PQSignature.Binding.valueToEntry(signature)) != null) {
                                do {
                                    val tupleId = LongBinding.compressedEntryToLong(cursor.value)
                                    val value = this.columnTx.get(tupleId) as VectorValue<*>
                                    val distance = this.predicate.distance(query, value)
                                    this.selection.offer(StandaloneRecord(tupleId, produces, arrayOf(distance)))
                                } while (cursor.nextDup)
                            }
                        }
                    }
                }
            }
        }

        /**
         * Partitioned filtering is not supported by [PQIndex].
         *
         * @param predicate The [Predicate] for the lookup
         * @param partition The [LongRange] specifying the [TupleId]s that should be considered.
         * @return The resulting [Iterator]
         */
        override fun filter(predicate: Predicate, partition: LongRange): Cursor<Record> {
            throw UnsupportedOperationException("The PQIndex does not support ranged filtering!")
        }

        /**
         * Collects and returns a subset of the available data for learning and training.
         *
         * @param txn The [EntityTx] used to obtain the learning data.
         * @return List of [Record]s used for learning.
         */
        private fun acquireLearningData(txn: ColumnTx<*>, random: RandomGenerator): List<VectorValue<*>> {
            val learningData = LinkedList<VectorValue<*>>()
            val learningDataFraction = this@Tx.config.samples.toDouble() / txn.count()
            val cursor = txn.cursor()
            while (cursor.hasNext()) {
                if (random.nextDouble() <= learningDataFraction) {
                    val value = cursor.value()
                    if (value is VectorValue<*>) {
                        learningData.add(value)
                    }
                }
            }
            cursor.close()
            return learningData
        }
    }
}
