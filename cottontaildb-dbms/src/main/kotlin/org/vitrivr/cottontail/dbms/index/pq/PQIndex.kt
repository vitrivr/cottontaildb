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
import org.vitrivr.cottontail.core.values.types.NumericValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.VectorValue
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.execution.operators.sort.RecordComparator
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.index.*
import org.vitrivr.cottontail.dbms.index.va.VAFIndex
import org.vitrivr.cottontail.dbms.operations.Operation
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

        /**
         * Opens a [PQIndex] for the given [Name.IndexName] in the given [DefaultEntity].
         *
         * @param name The [Name.IndexName] of the [PQIndex].
         * @param entity The [DefaultEntity] that holds the [PQIndex].
         * @return The opened [PQIndex]
         */
        override fun open(name: Name.IndexName, entity: DefaultEntity): PQIndex = PQIndex(name, entity)

        /**
         * Tries to initialize the [Store] for a [PQIndex].
         *
         * @param name The [Name.IndexName] of the [PQIndex].
         * @param entity The [DefaultEntity] that holds the [PQIndex].
         * @return True on success, false otherwise.
         */
        override fun initialize(name: Name.IndexName, entity: DefaultEntity.Tx): Boolean {
            val store = entity.dbo.catalogue.environment.openStore(name.storeName(), StoreConfig.WITH_DUPLICATES, entity.context.xodusTx, true)
            return store != null
        }

        /**
         * Generates and returns a [PQIndexConfig] for the given [parameters] (or default values, if [parameters] are not set).
         *
         * @param parameters The parameters to initialize the default [PQIndexConfig] with.
         */
        override fun buildConfig(parameters: Map<String, String>): IndexConfig<PQIndex> = PQIndexConfig(
            parameters[PQIndexConfig.KEY_DISTANCE]?.let { Name.FunctionName(it) } ?: EuclideanDistance.FUNCTION_NAME,
            parameters[PQIndexConfig.KEY_SAMPLE_SIZE]?.toInt() ?: 1500,
            parameters[PQIndexConfig.KEY_NUM_CENTROIDS]?.toInt() ?: 100,
            parameters[PQIndexConfig.KEY_NUM_SUBSPACES]?.toInt()
        )

        /**
         * Returns the [PQIndexConfig.Binding]
         *
         * @return [PQIndexConfig.Binding]
         */
        override fun configBinding(): ComparableBinding = PQIndexConfig.Binding
    }

    /** The [IndexType] of this [PQIndex]. */
    override val type: IndexType
        get() = IndexType.PQ

    /** False since [PQIndex] currently doesn't support incremental updates. */
    override val supportsIncrementalUpdate: Boolean = false

    /** True since [PQIndex] supports partitioning. */
    override val supportsPartitioning: Boolean = false

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

        init {
            val config = this.config
            this.supportedDistances = setOf(Signature.Closed(config.distance, arrayOf(this.column.type, this.column.type), Types.Double))
        }

        /** The Xodus [Store] used to store [PQSignature]s. */
        private var dataStore: Store = this@PQIndex.catalogue.environment.openStore(this@PQIndex.name.storeName(), StoreConfig.WITH_DUPLICATES, this.context.xodusTx, false)
            ?: throw DatabaseException.DataCorruptionException("Data store for index ${this@PQIndex.name} is missing.")

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
            val subspaces = this.config.numSubspaces ?: ProductQuantizer.defaultNumberOfSubspaces(predicate.column.type.logicalSize)
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
            val indexedColumn = this.columns[0]
            val type = indexedColumn.type as Types.Vector<*,*>
            val distanceFunction = this@PQIndex.catalogue.functions.obtain(Signature.SemiClosed(config.distance, arrayOf(Argument.Typed(type), Argument.Typed(type)))) as VectorDistance<*>
            val newPq = ProductQuantizer.learnFromData(distanceFunction, this.acquireLearningData(entityTx, random), config)

            /* Clear old signatures. */
            this.clear()

            /* Iterate over entity and update index with entries. */
            val cursor = entityTx.cursor(arrayOf(indexedColumn))
            cursor.forEach { rec ->
                val value = rec[indexedColumn]
                if (value is VectorValue<*>) {
                    val sig = newPq.quantize(value)
                    this.dataStore.put(this.context.xodusTx, PQSignature.Binding.valueToEntry(sig), rec.tupleId.toKey())
                }
            }

            /* Close cursor. */
            cursor.close()

            /* Update index state for index. */
            this.updateState(IndexState.CLEAN, this.config.copy(centroids = newPq.centroids()))
            LOGGER.debug("Rebuilding PQIndex {} completed!", this@PQIndex.name)
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
         * Clears the [VAFIndex] underlying this [Tx] and removes all entries it contains.
         */
        override fun clear() = this.txLatch.withLock {
            /* Truncate and replace store.*/
            this@PQIndex.catalogue.environment.truncateStore(this@PQIndex.name.storeName(), this.context.xodusTx)
            this.dataStore = this@PQIndex.catalogue.environment.openStore(this@PQIndex.name.storeName(), StoreConfig.WITH_DUPLICATES, this.context.xodusTx, false)
                ?: throw DatabaseException.DataCorruptionException("Data store for index ${this@PQIndex.name} is missing.")

            /* Update catalogue entry for index. */
            this.updateState(IndexState.STALE)
        }

        /**
         *
         */
        override fun tryApply(operation: Operation.DataManagementOperation.InsertOperation): Boolean {
            return false
        }

        /**
         *
         */
        override fun tryApply(operation: Operation.DataManagementOperation.UpdateOperation): Boolean {
            return false
        }

        /**
         *
         */
        override fun tryApply(operation: Operation.DataManagementOperation.DeleteOperation): Boolean {
            return false
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

                /** The [ProductQuantizer] instance used for this [Cursor]. */
                private val pq = ProductQuantizer.loadFromConfig(this.predicate.distance, this.config)

                /** Internal [EntityTx] used to access actual values. */
                private val entityTx = this@Tx.context.getTx(this@PQIndex.parent) as EntityTx

                /** Prepares [PQLookupTable]s for the given query vector(s). */
                private val lookupTable: PQLookupTable = this.pq.createLookupTable(this.predicate.query.value as VectorValue<*>)

                /** The [HeapSelection] use for finding the top k entries. */
                private var selection = HeapSelection(this.predicate.k.toLong(), RecordComparator.SingleNonNullColumnComparator(this.predicate.distanceColumn, SortOrder.ASCENDING))

                /** The current [Cursor] position. */
                private var position = -1L

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
                    val preKnnSize = (this.predicate.k * 1.15).toLong() /* Pre-kNN size is 15% larger than k. */
                    val preKnn = HeapSelection(preKnnSize, Comparator<Pair<LongArray, Double>> { o1, o2 -> o1.second.compareTo(o2.second) })
                    val produces = this@Tx.columnsFor(predicate).toTypedArray()

                    /* Phase 1: Perform pre-NNS based on signatures. */
                    val subTx = this@Tx.context.xodusTx.readonlySnapshot
                    val cursor = this@Tx.dataStore.openCursor(subTx)
                    while (cursor.next) {
                        val signature = PQSignature.Binding.entryToValue(cursor.key)
                        val approximation = this.lookupTable.approximateDistance(signature)
                        if (preKnn.size < this.predicate.k || preKnn.peek()!!.second > approximation) {
                            val tupleIds = mutableListOf<TupleId>()
                            do {
                                tupleIds.add(LongBinding.compressedEntryToLong(cursor.value))
                            } while (cursor.nextDup) /* Collect all duplicates. */
                            preKnn.offer(tupleIds.toLongArray() to approximation)
                        }
                    }

                    /* Closes the cursor. */
                    cursor.close()
                    subTx.abort()

                    /* Phase 2: Perform exact kNN based on pre-kNN results. */
                    val query = this.predicate.query.value as VectorValue<*>
                    for (j in 0 until preKnn.size) {
                        val tupleIds = preKnn[j].first
                        for (tupleId in tupleIds) {
                            val value = this.entityTx.read(tupleId, this@Tx.columns)[this@Tx.columns[0]]
                            if (value is VectorValue<*>) {
                                val distance = this.predicate.distance(query, value)
                                if (distance != null && (this.selection.added < this.predicate.k || (this.selection.peek()!![this.predicate.distanceColumn] as NumericValue<*>).value.toDouble() > distance.value)) {
                                    this.selection.offer(StandaloneRecord(tupleId, produces, arrayOf(distance)))
                                }
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
        private fun acquireLearningData(txn: EntityTx, random: RandomGenerator): List<VectorValue<*>> {
            val learningData = LinkedList<VectorValue<*>>()
            val learningDataFraction = this@Tx.config.sampleSize.toDouble() / txn.count()
            val cursor = txn.cursor(this.columns)
            cursor.forEach {
                if (random.nextDouble() <= learningDataFraction) {
                    val value = it[this.columns[0]]
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
