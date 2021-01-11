package org.vitrivr.cottontail.database.index.pq

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.mapdb.DB
import org.mapdb.Serializer
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.column.*
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.events.DataChangeEvent
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.index.va.VAFIndex
import org.vitrivr.cottontail.database.queries.components.AtomicBooleanPredicate
import org.vitrivr.cottontail.database.queries.components.KnnPredicate
import org.vitrivr.cottontail.database.queries.components.Predicate
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.math.knn.selection.ComparablePair
import org.vitrivr.cottontail.math.knn.selection.MinHeapSelection
import org.vitrivr.cottontail.math.knn.selection.MinSingleSelection
import org.vitrivr.cottontail.model.basics.*
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.IntValue
import org.vitrivr.cottontail.model.values.types.VectorValue
import org.vitrivr.cottontail.utilities.extensions.write
import java.lang.Math.floorDiv
import java.nio.file.Path
import java.util.*
import kotlin.collections.ArrayDeque

/**
 * author: Gabriel Zihlmann
 * date: 25.8.2020
 *
 * Todo: * signatures: Ints are convenient but wasting a lot of space...
 *         we should move towards only using as many bits as necessary...
 *       * avoid copying
 *       * generalize to other datatypes than Complex32VV
 *
 * changes 13.10.2020:
 * * permutation of dimensions will no longer be applied. PQ is 5-10% more accurate without it!
 * * quantizing complex vectors directly is possible and about as accurate as real vectors. This has been changed
 * * in this class now. Performance implications need to be assessed
 *
 * 22.10.2020:
 * * deduplication is implemented. Brings significant speedups for lower subspace and centroid counts (2-3x)
 * * beneficial to go to higher k in approximate scan. This requires more effort on re-ranking the approx
 *   matches (can become dominant and take a multiple of the time to scan without parallelization).
 *   This has been parallelized now.
 */
class PQIndex(override val name: Name.IndexName, override val parent: Entity, override val columns: Array<ColumnDef<*>>, config: PQIndexConfig? = null): Index() {
    companion object {
        const val CONFIG_NAME = "pq_config"
        const val PQ_NAME_REAL = "pq_cb_real"
        const val SIG_NAME = "pq_sig"
        val LOGGER = LoggerFactory.getLogger(PQIndex::class.java)!!

        /**
         * The index of the permutation array is the index in the unpermuted space
         * The value at that index is to which dimension it was permuted in the permuted space
         */
        fun generateRandomPermutation(size: Int, rng: SplittableRandom): Pair<IntArray, IntArray> {
            // init permutation of dimensions as identity permutation
            val permutation = IntArray(size) { it }
            // fisher-yates shuffle
            for (i in 0 until size - 1) {
                val toSwap = i + rng.nextInt(size - i)
                val h = permutation[toSwap]
                permutation[toSwap] = permutation[i]
                permutation[i] = h
            }
            val reversePermutation = IntArray(permutation.size) {
                permutation.indexOf(it)
            }
            return permutation to reversePermutation
        }
    }

    /** The [Path] to the [DBO]'s main file OR folder. */
    override val path: Path = this.parent.path.resolve("idx_pq_$name.db")

    /** The [PQIndex] implementation returns exactly the columns that is indexed. */
    override val produces: Array<ColumnDef<*>> = arrayOf(ColumnDef(this.parent.name.column("distance"), ColumnType.forName("DOUBLE")))

    /** The type of [Index]. */
    override val type = IndexType.PQ

    /** False since [PQIndex] currently doesn't support incremental updates. */
    override val supportsIncrementalUpdate: Boolean = false

    /** The internal [DB] reference. */
    private val db: DB = this.parent.parent.parent.config.mapdb.db(this.path)

    /** The [PQIndexConfig] used by this [PQIndex] instance. */
    private val config: PQIndexConfig

    /** The [PQ] instance used for real vector components. */
    private val pqStore = this.db.atomicVar(PQ_NAME_REAL, PQ.Serializer).createOrOpen()

    /** The store of signatures. */
    private val signaturesStore = this.db.hashMap(SIG_NAME, PQSignature.Serializer, Serializer.LONG_ARRAY).counterEnable().createOrOpen()

    init {
        require(this.columns.size == 1) { "PQIndex only supports indexing a single column." }

        /* Load or create config. */
        val configOnDisk = this.db.atomicVar(CONFIG_NAME, PQIndexConfig.Serializer).createOrOpen()
        if (configOnDisk.get() == null) {
            if (config != null) {
                this.config = config
            } else {
                this.config = PQIndexConfig(10, 1, 5e-3, System.currentTimeMillis())
            }
            configOnDisk.set(this.config)
        } else {
            this.config = configOnDisk.get()
        }
        this.db.commit()

        /** Some assumptions and sanity checks. Some are for documentation, some are cheap enough to actually keep and check. */
        require(this.config.numCentroids > 0) { "PQIndex supports a maximum number of ${this.config.numCentroids} centroids." }
        require(this.config.numCentroids <= Short.MAX_VALUE)
        require(this.config.numSubspaces > 0) { "PQIndex requires at least one centroid." }
        require(this.columns[0].logicalSize >= this.config.numSubspaces) { "Logical size of the column must be greater or equal than the number of subspaces." }
        require(this.columns[0].logicalSize % this.config.numSubspaces == 0) { "Logical size of the column modulo the number of subspaces must be zero." }
    }

    /**
     * Flag indicating if this [PQIndex] has been closed.
     */
    @Volatile
    override var closed: Boolean = false
        private set

    /**
     * Closes this [PQIndex] and the associated data structures.
     */
    override fun close() = this.closeLock.write {
        if (!closed) {
            db.close()
            closed = true
        }
    }

    /**
     * Checks if this [Index] can process the provided [Predicate] and returns true if so and false otherwise.
     *
     * @param predicate [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate) =
            predicate is KnnPredicate<*>
                    && predicate.columns.first() == this.columns[0]

    /**
     * Calculates the cost estimate if this [Index] processing the provided [Predicate].
     *
     * TODO: Calculate actual cost.
     *
     * @param predicate [Predicate] to check.
     * @return [Cost] estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate) = Cost.ZERO

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [PQIndex].
     *
     * @param context The [TransactionContext] to create this [IndexTx] for.
     */
    override fun newTx(context: TransactionContext): IndexTx = Tx(context)

    /**
     * A [IndexTx] that affects this [Index].
     */
    private inner class Tx(context: TransactionContext) : Index.Tx(context) {
        /**
         *
         */
        override fun rebuild() {
            /* Obtain some learning data for training. */
            LOGGER.info("Rebuilding PQIndex:")
            LOGGER.debug("Collecting training data...")
            val txn = this.context.getTx(this.dbo.parent) as EntityTx
            val data = this.acquireLearningData(txn)

            /* Obtain PQ data structure... */
            LOGGER.debug("Training quantizer...")
            val pq = PQ.fromData(this@PQIndex.config, this@PQIndex.columns[0], data)

            /* ... and generate signatures. */
            LOGGER.debug("Generating signatures...")
            val signatureMap = Object2ObjectOpenHashMap<PQSignature, MutableList<TupleId>>()
            txn.scan(this.columns).forEach { rec ->
                val value = rec[this@PQIndex.columns[0]]
                if (value is VectorValue<*>) {
                    val sig = pq.getSignature(value)
                    signatureMap.compute(sig) { _, old ->
                        val ret = old ?: mutableListOf<TupleId>()
                        ret.add(rec.tupleId)
                        ret
                    }
                }
            }

            /* Store PQ and signatures in surrounding index. */
            this@PQIndex.pqStore.set(pq)
            this@PQIndex.signaturesStore.clear()
            signatureMap.forEach { (k, v) ->
                this@PQIndex.signaturesStore[k] = v.toLongArray()
            }
        }

        /**
         * Updates the [PQIndex] with the provided [DataChangeEvent]s. This method determines,
         * whether the [Record] affected by the [DataChangeEvent] should be added or updated
         *
         * @param update Collection of [DataChangeEvent]s to process.
         */
        override fun update(update: Collection<DataChangeEvent>) {
            TODO("Not yet implemented")
        }

        /**
         * Performs a lookup through this [PQIndex.Tx] and returns a [CloseableIterator] of all [Record]s
         * that match the [Predicate]. Only supports [KnnPredicate]s.
         *
         * <strong>Important:</strong> The [CloseableIterator] is not thread safe! It remains to the
         * caller to close the [CloseableIterator]
         *
         * @param predicate The [Predicate] for the lookup
         * @return The resulting [CloseableIterator]
         */
        override fun filter(predicate: Predicate): CloseableIterator<Record> = object : CloseableIterator<Record> {

            /** Cast [AtomicBooleanPredicate] (if such a cast is possible).  */
            private val predicate = if (predicate is KnnPredicate<*>) {
                predicate
            } else {
                throw QueryException.UnsupportedPredicateException("Index '${this@PQIndex.name}' (PQ Index) does not support predicates of type '${predicate::class.simpleName}'.")
            }

            /** The [PQ] instance used for this [CloseableIterator]. */
            private val pq = this@PQIndex.pqStore.get()

            /** Prepares [PQLookupTable]s for the given query vector(s). */
            private val lookupTables = this.predicate.query.map {
                this.pq.getLookupTable(it, this.predicate.distance)
            }

            /** The [ArrayDeque] of [StandaloneRecord] produced by this [VAFIndex]. Evaluated lazily! */
            private val resultsQueue: ArrayDeque<StandaloneRecord> by lazy {
                prepareResults()
            }

            /** Flag indicating whether this [CloseableIterator] has been closed. */
            @Volatile
            private var closed = false

            init {
                this@Tx.withReadLock { }
            }

            override fun hasNext(): Boolean = this.resultsQueue.isNotEmpty()

            override fun next(): Record = this.resultsQueue.removeFirst()

            override fun close() {
                if (!this.closed) {
                    this.resultsQueue.clear()
                    this.closed = true
                }
            }

            /**
             * Executes the kNN and prepares the results to return by this [CloseableIterator].
             */
            private fun prepareResults(): ArrayDeque<StandaloneRecord> {
                /* Prepare data structures for NNS. */
                val txn = this@Tx.context.getTx(this@PQIndex.parent) as EntityTx
                val tuplesPerSignature = floorDiv(txn.count(), this@PQIndex.signaturesStore.sizeLong())
                val approxKnn = if (this.predicate.k > tuplesPerSignature) {
                    (this.predicate.k / tuplesPerSignature) * 100
                } else {
                    100
                }.toInt()
                val preKnns = Array(this.predicate.query.size) { MinHeapSelection<ComparablePair<PQSignature, Double>>(approxKnn) }

                /* Phase 1: Perform pre-kNN based on signatures. */
                for (k in this@PQIndex.signaturesStore.keys) {
                    for (queryIndex in this.predicate.query.indices) {
                        val approximation = this.lookupTables[queryIndex].approximateDistance(k)
                        if (preKnns[queryIndex].size < this.predicate.k || preKnns[queryIndex].peek()!!.second > approximation) {
                            preKnns[queryIndex].offer(ComparablePair(k, approximation))
                        }
                    }
                }

                /* Phase 2: Perform exact kNN based on pre-kNN results. */
                val knns = if (this.predicate.k == 1) {
                    Array(this.predicate.query.size) { MinSingleSelection<ComparablePair<TupleId, DoubleValue>>() }
                } else {
                    Array(this.predicate.query.size) { MinHeapSelection<ComparablePair<TupleId, DoubleValue>>(this.predicate.k) }
                }
                for ((queryIndex, query) in this.predicate.query.withIndex()) {
                    for (j in 0 until preKnns[queryIndex].size) {
                        val tupleIds = this@PQIndex.signaturesStore[preKnns[queryIndex][j].first]!!
                        for (tupleId in tupleIds) {
                            val exact = txn.read(tupleId, this@PQIndex.columns)[this@PQIndex.columns[0]]
                            if (exact is VectorValue<*>) {
                                val distance = this.predicate.distance(exact, query)
                                if (knns[queryIndex].size < this.predicate.k || knns[queryIndex].peek()!!.second > distance) {
                                    knns[queryIndex].offer(ComparablePair(tupleId, distance))
                                }
                            }
                        }
                    }
                }

                /* Phase 3: Prepare and return list of results. */
                val queue = ArrayDeque<StandaloneRecord>(this.predicate.k * this.predicate.query.size)
                for ((queryIndex, knn) in knns.withIndex()) {
                    for (i in 0 until knn.size) {
                        queue.add(StandaloneRecord(knn[i].first, this@PQIndex.produces, arrayOf(IntValue(queryIndex), knn[i].second)))
                    }
                }
                return queue
            }
        }

        /**
         * Commits changes to the [PQIndex].
         */
        override fun performCommit() {
            this@PQIndex.db.commit()
        }

        /**
         * Makes a rollback on all changes to the [PQIndex].
         */
        override fun performRollback() {
            this@PQIndex.db.rollback()
        }

        /**
         * Collects and returns a subset of the available data for learning and training.
         *
         * @param txn The [EntityTx] used to obtain the learning data.
         * @return List of [Record]s used for learning.
         */
        private fun acquireLearningData(txn: EntityTx): List<VectorValue<*>> {
            val learningData = mutableListOf<VectorValue<*>>()
            val rng = SplittableRandom(this@PQIndex.config.seed)
            txn.scan(this.columns).forEach {
                if (rng.nextDouble() <= this@PQIndex.config.learningDataFraction) {
                    val value = it[this@PQIndex.columns[0]]
                    if (value is VectorValue<*>) {
                        learningData.add(value)
                    }
                }
            }
            return learningData
        }
    }
}
