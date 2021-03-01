package org.vitrivr.cottontail.database.index.pq

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.column.*
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.events.DataChangeEvent
import org.vitrivr.cottontail.database.index.AbstractIndex
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.index.va.VAFIndex
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
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
import org.vitrivr.cottontail.utilities.math.KnnUtilities
import java.nio.file.Path
import java.util.*
import kotlin.collections.ArrayDeque

/**
 * An [AbstractIndex] structure for nearest neighbor search (NNS) that uses a product quantization (PQ). Can
 * be used for all type of [VectorValue]s and distance metrics.
 *
 * TODO: Check if generalization to other distance metrics is actually valid.
 *
 * References:
 * [1] Guo, Ruiqi, et al. "Quantization based fast inner product search." Artificial Intelligence and Statistics. 2016.
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 2.0.1
 */
class PQIndex(path: Path, parent: DefaultEntity, config: PQIndexConfig? = null) : AbstractIndex(path, parent) {

    companion object {
        private const val PQ_INDEX_FIELD = "cdb_pq_real"
        private const val PQ_INDEX_SIGNATURES_FIELD = "cdb_pq_signatures"
        val LOGGER = LoggerFactory.getLogger(PQIndex::class.java)!!


        /**
         * Dynamically determines the number of subspaces for the given dimension.
         *
         * @param d The dimensionality of the vector.
         * @return Number of subspaces to use.
         */
        fun defaultNumberOfSubspaces(d: Int): Int {
            var subspaces: Int = when {
                d == 1 -> 1
                d == 2 -> 2
                d <= 8 -> 4
                d <= 64 -> 4
                d <= 256 -> 8
                d <= 1024 -> 16
                d <= 4096 -> 32
                else -> 64
            }
            while (subspaces < d && subspaces < Byte.MAX_VALUE) {
                if (d % subspaces == 0) {
                    return subspaces
                }
                subspaces++
            }
            throw IllegalArgumentException("")
        }
    }

    /** The [PQIndex] implementation returns exactly the columns that is indexed. */
    override val produces: Array<ColumnDef<*>> = arrayOf(
        KnnUtilities.queryIndexColumnDef(this.parent.name),
        KnnUtilities.distanceColumnDef(this.parent.name)
    )

    /** The type of [AbstractIndex]. */
    override val type = IndexType.PQ

    /** The [PQIndexConfig] used by this [PQIndex] instance. */
    override val config: PQIndexConfig

    /** The [PQ] instance used for real vector components. */
    private val pqStore = this.store.atomicVar(PQ_INDEX_FIELD, PQ.Serializer).createOrOpen()

    /** The store of [PQIndexEntry]. */
    private val signaturesStore =
        this.store.indexTreeList(PQ_INDEX_SIGNATURES_FIELD, PQIndexEntry.Serializer).createOrOpen()

    init {
        /* Load or create config. */
        require(this.columns.size == 1) { "PQIndex only supports indexing a single column." }
        val configOnDisk =
            this.store.atomicVar(INDEX_CONFIG_FIELD, PQIndexConfig.Serializer).createOrOpen()
        if (configOnDisk.get() == null) {
            if (config != null) {
                if (config.numSubspaces == PQIndexConfig.AUTO_VALUE || (config.numSubspaces % this.columns[0].type.logicalSize) != 0) {
                    this.config =
                        config.copy(numSubspaces = defaultNumberOfSubspaces(this.columns[0].type.logicalSize))
                } else {
                    this.config = config
                }
            } else {
                this.config = PQIndexConfig(10, 500, 5000, System.currentTimeMillis())
            }
            configOnDisk.set(this.config)
        } else {
            this.config = configOnDisk.get()
        }
        this.store.commit()

        /** Some assumptions and sanity checks. Some are for documentation, some are cheap enough to actually keep and check. */
        require(this.config.numCentroids > 0) { "PQIndex supports a maximum number of ${this.config.numCentroids} centroids." }
        require(this.config.numCentroids <= Short.MAX_VALUE)
        require(this.config.numSubspaces > 0) { "PQIndex requires at least one centroid." }
        require(this.columns[0].type.logicalSize >= this.config.numSubspaces) { "Logical size of the column must be greater or equal than the number of subspaces." }
        require(this.columns[0].type.logicalSize % this.config.numSubspaces == 0) { "Logical size of the column modulo the number of subspaces must be zero." }
    }

    /** False since [PQIndex] currently doesn't support incremental updates. */
    override val supportsIncrementalUpdate: Boolean = false

    /** True since [PQIndex] supports partitioning. */
    override val supportsPartitioning: Boolean = true

    /**
     * Checks if this [AbstractIndex] can process the provided [Predicate] and returns true if so and false otherwise.
     *
     * @param predicate [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate) =
        predicate is KnnPredicate
                && predicate.columns.first() == this.columns[0]

    /**
     * Calculates the cost estimate if this [AbstractIndex] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return [Cost] estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate) =
        if (predicate is KnnPredicate && predicate.column == this.columns[0]) {
            Cost(
                this.signaturesStore.size * this.config.numSubspaces * Cost.COST_DISK_ACCESS_READ + predicate.query.size * predicate.k * predicate.column.type.logicalSize * Cost.COST_DISK_ACCESS_READ,
                predicate.query.size * (this.signaturesStore.size * (4 * Cost.COST_MEMORY_ACCESS + Cost.COST_FLOP) + predicate.k * predicate.atomicCpuCost),
                (predicate.query.size * predicate.k * this.produces.map { it.type.physicalSize }
                    .sum()).toFloat()
            )
        } else {
            Cost.INVALID
        }

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [PQIndex].
     *
     * @param context The [TransactionContext] to create this [IndexTx] for.
     */
    override fun newTx(context: TransactionContext): IndexTx = Tx(context)

    /**
     * A [IndexTx] that affects this [AbstractIndex].
     */
    private inner class Tx(context: TransactionContext) : AbstractIndex.Tx(context) {

        /**
         * Returns the number of [PQIndexEntry]s in this [PQIndex]
         *
         * @return The number of [PQIndexEntry] stored in this [PQIndex]
         */
        override fun count(): Long = this.withReadLock {
            this@PQIndex.signaturesStore.size.toLong()
        }

        /**
         * Rebuilds the surrounding [PQIndex] from scratch, thus re-creating the the [PQ] codebook
         * with a new, random sample and re-calculating all the signatures. This method can
         * take a while to complete!
         */
        override fun rebuild() = this.withWriteLock {
            /* Obtain some learning data for training. */
            LOGGER.debug("Rebuilding PQ index {}", this@PQIndex.name)
            val txn = this.context.getTx(this.dbo.parent) as EntityTx
            val data = this.acquireLearningData(txn)

            /* Obtain PQ data structure... */
            val pq = PQ.fromData(this@PQIndex.config, this@PQIndex.columns[0], data)

            /* ... and generate signatures. */
            val signatureMap = Object2ObjectOpenHashMap<PQSignature, LinkedList<TupleId>>(txn.count().toInt())
            txn.scan(this.columns).forEach { rec ->
                val value = rec[this@PQIndex.columns[0]]
                if (value is VectorValue<*>) {
                    val sig = pq.getSignature(value)
                    signatureMap.compute(sig) { _, v ->
                        val ret = v ?: LinkedList<TupleId>()
                        ret.add(rec.tupleId)
                        ret
                    }
                }
            }

            /* Now persist everything. */
            this@PQIndex.pqStore.set(pq)
            this@PQIndex.signaturesStore.clear()
            for (entry in signatureMap.entries) {
                this@PQIndex.signaturesStore.add(PQIndexEntry(entry.key, entry.value.toLongArray()))
            }
            this@PQIndex.dirtyField.compareAndSet(true, false)
            LOGGER.debug("Rebuilding PQIndex {} complete.", this@PQIndex.name)
        }

        /**
         * Updates the [PQIndex] with the provided [DataChangeEvent]s. Since the [PQIndex] does
         * not support incremental updates, calling this method will simply set the [PQIndex] [dirty]
         * flag to true.
         *
         * @param event Collection of [DataChangeEvent]s to process.
         */
        override fun update(event: DataChangeEvent) = this.withWriteLock {
            this@PQIndex.dirtyField.compareAndSet(false, true)
            Unit
        }

        /**
         * Clears the [PQIndex] underlying this [Tx] and removes all entries it contains.
         */
        override fun clear() = this.withWriteLock {
            this@PQIndex.dirtyField.compareAndSet(false, true)
            this@PQIndex.signaturesStore.clear()
        }

        /**
         * Performs a lookup through this [PQIndex.Tx] and returns a [Iterator] of all [Record]s
         * that match the [Predicate]. Only supports [KnnPredicate]s.
         *
         * <strong>Important:</strong> The [Iterator] is not thread safe! It remains to the
         * caller to close the [Iterator]
         *
         * @param predicate The [Predicate] for the lookup
         * @return The resulting [Iterator]
         */
        override fun filter(predicate: Predicate): Iterator<Record> =
            filterRange(predicate, 0L until this.count())

        /**
         * Performs a lookup through this [PQIndex.Tx] and returns a [Iterator] of all [Record]s
         * that match the [Predicate] in the given [LongRange]. Only supports [KnnPredicate]s.
         *
         * <strong>Important:</strong> The [Iterator] is not thread safe!
         *
         * @param predicate The [Predicate] for the lookup
         * @param range The [LongRange] of [PQIndexEntry] to consider.
         * @return The resulting [Iterator]
         */
        override fun filterRange(predicate: Predicate, range: LongRange) =
            object : Iterator<Record> {

                /** Cast [KnnPredicate] (if such a cast is possible).  */
                private val predicate = if (predicate is KnnPredicate) {
                    predicate
                } else {
                    throw QueryException.UnsupportedPredicateException("Index '${this@PQIndex.name}' (PQ Index) does not support predicates of type '${predicate::class.simpleName}'.")
                }

                /** The [PQ] instance used for this [Iterator]. */
                private val pq = this@PQIndex.pqStore.get()

            /** Prepares [PQLookupTable]s for the given query vector(s). */
            private val lookupTables = this.predicate.query.map {
                this.pq.getLookupTable(it, this.predicate.distance)
            }

            /** The [ArrayDeque] of [StandaloneRecord] produced by this [VAFIndex]. Evaluated lazily! */
            private val resultsQueue: ArrayDeque<StandaloneRecord> by lazy {
                prepareResults()
            }

            init {
                this@Tx.withReadLock { }
            }

            override fun hasNext(): Boolean = this.resultsQueue.isNotEmpty()

            override fun next(): Record = this.resultsQueue.removeFirst()

            /**
             * Executes the kNN and prepares the results to return by this [Iterator].
             */
            private fun prepareResults(): ArrayDeque<StandaloneRecord> {
                /* Prepare data structures for NNS. */
                val txn = this@Tx.context.getTx(this@PQIndex.parent) as EntityTx
                val preKnnSize =
                    (this.predicate.k * 1.15).toInt() /* Pre-kNN size is 15% larger than k. */
                val preKnns = Array(this.predicate.query.size) {
                    MinHeapSelection<ComparablePair<LongArray, Double>>(preKnnSize)
                }

                /* Phase 1: Perform pre-kNN based on signatures. */
                for (i in range) {
                    val entry = this@PQIndex.signaturesStore[i.toInt()]
                    for (queryIndex in this.predicate.query.indices) {
                        val approximation =
                            this.lookupTables[queryIndex].approximateDistance(entry!!.signature)
                        if (preKnns[queryIndex].size < this.predicate.k || preKnns[queryIndex].peek()!!.second > approximation) {
                            preKnns[queryIndex].offer(ComparablePair(entry.tupleIds, approximation))
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
                        val tupleIds = preKnns[queryIndex][j].first
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
         * Collects and returns a subset of the available data for learning and training.
         *
         * @param txn The [EntityTx] used to obtain the learning data.
         * @return List of [Record]s used for learning.
         */
        private fun acquireLearningData(txn: EntityTx): List<VectorValue<*>> {
            val learningData = LinkedList<VectorValue<*>>()
            val rng = SplittableRandom(this@PQIndex.config.seed)
            val learningDataFraction = this@PQIndex.config.sampleSize.toDouble() / txn.count()
            txn.scan(this.columns).forEach {
                if (rng.nextDouble() <= learningDataFraction) {
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
