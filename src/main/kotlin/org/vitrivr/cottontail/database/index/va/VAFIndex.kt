package org.vitrivr.cottontail.database.index.va

import org.mapdb.Atomic
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.column.ColumnDef
import org.vitrivr.cottontail.database.entity.DefaultEntity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.events.DataChangeEvent
import org.vitrivr.cottontail.database.index.AbstractIndex
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.index.va.bounds.*
import org.vitrivr.cottontail.database.index.va.signature.Marks
import org.vitrivr.cottontail.database.index.va.signature.MarksGenerator
import org.vitrivr.cottontail.database.index.va.signature.VAFSignature
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.database.queries.predicates.Predicate
import org.vitrivr.cottontail.database.queries.predicates.knn.KnnPredicate
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.math.knn.metrics.EuclidianDistance
import org.vitrivr.cottontail.math.knn.metrics.ManhattanDistance
import org.vitrivr.cottontail.math.knn.metrics.MinkowskiDistance
import org.vitrivr.cottontail.math.knn.metrics.SquaredEuclidianDistance
import org.vitrivr.cottontail.math.knn.selection.ComparablePair
import org.vitrivr.cottontail.math.knn.selection.MinHeapSelection
import org.vitrivr.cottontail.math.knn.selection.MinSingleSelection
import org.vitrivr.cottontail.model.basics.Record
import org.vitrivr.cottontail.model.exceptions.QueryException
import org.vitrivr.cottontail.model.recordset.StandaloneRecord
import org.vitrivr.cottontail.model.values.DoubleValue
import org.vitrivr.cottontail.model.values.IntValue
import org.vitrivr.cottontail.model.values.types.RealVectorValue
import org.vitrivr.cottontail.model.values.types.VectorValue
import org.vitrivr.cottontail.utilities.extensions.write
import org.vitrivr.cottontail.utilities.math.KnnUtilities
import java.nio.file.Path
import java.util.*
import kotlin.collections.ArrayDeque
import kotlin.math.max
import kotlin.math.min

/**
 * An [AbstractIndex] structure for nearest neighbor search (NNS) that uses a vector approximation (VA) file ([1]).
 * Can be used for all types of [RealVectorValue]s and all [MinkowskiDistance] metrics.
 *
 * References:
 * [1] Weber, R. and Blott, S., 1997. An approximation based data structure for similarity search (No. 9141, p. 416). Technical Report 24, ESPRIT Project HERMES.
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 2.0.0
 */
class VAFIndex(path: Path, parent: DefaultEntity, config: VAFIndexConfig? = null) : AbstractIndex(path, parent) {

    companion object {
        private const val VAF_INDEX_SIGNATURES_FIELD = "vaf_signatures"
        private const val VAF_INDEX_MARKS_FIELD = "vaf_marks"
        val LOGGER: Logger = LoggerFactory.getLogger(VAFIndex::class.java)
    }

    /** The [VAFIndex] implementation returns exactly the columns that is indexed. */
    override val produces: Array<ColumnDef<*>> = arrayOf(
        KnnUtilities.queryIndexColumnDef(this.parent.name),
        KnnUtilities.distanceColumnDef(this.parent.name)
    )

    /** The type of [AbstractIndex]. */
    override val type = IndexType.VAF

    /** The [VAFIndexConfig] used by this [VAFIndex] instance. */
    override val config: VAFIndexConfig

    /** Store for the [Marks]. */
    private val marksStore: Atomic.Var<Marks> =
        this.db.atomicVar(VAF_INDEX_MARKS_FIELD, Marks.Serializer).createOrOpen()

    /** Store for the signatures. */
    private val signatures =
        this.db.indexTreeList(VAF_INDEX_SIGNATURES_FIELD, VAFSignature.Serializer).createOrOpen()

    init {
        require(this.columns.size == 1) { "$VAFIndex only supports indexing a single column." }

        /* Load or create config. */
        val configOnDisk =
            this.db.atomicVar(INDEX_CONFIG_FIELD, VAFIndexConfig.Serializer).createOrOpen()
        if (configOnDisk.get() == null) {
            if (config != null) {
                this.config = config
            } else {
                this.config = VAFIndexConfig(50)
            }
            configOnDisk.set(this.config)
        } else {
            this.config = configOnDisk.get()
        }
        this.db.commit()
    }

    /** Flag indicating if this [VAFIndex] has been closed. */
    @Volatile
    override var closed: Boolean = false
        private set

    /** False since [VAFIndex] currently doesn't support incremental updates. */
    override val supportsIncrementalUpdate: Boolean = false

    /** True since [VAFIndex] supports partitioning. */
    override val supportsPartitioning: Boolean = true

    /**
     * Closes this [VAFIndex] and the associated data structures.
     */
    override fun close() = this.closeLock.write {
        if (!this.closed) {
            this.db.close()
            this.closed = true
        }
    }

    /**
     * Calculates the cost estimate if this [VAFIndex] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate) =
        if (predicate is KnnPredicate && predicate.column == this.columns[0] && (predicate.distance is MinkowskiDistance || predicate.distance is SquaredEuclidianDistance)) {
            Cost(
                this.signatures.size * this.columns[0].type.logicalSize * Cost.COST_DISK_ACCESS_READ + 0.1f * (this.signatures.size * predicate.query.size * this.columns[0].type.logicalSize * Cost.COST_DISK_ACCESS_READ),
                predicate.query.size * this.signatures.size * this.columns[0].type.logicalSize * (2*Cost.COST_DISK_ACCESS_READ + Cost.COST_FLOP) +  0.1f * this.signatures.size * predicate.query.size * predicate.cost,
                (predicate.query.size * predicate.k * this.produces.map { it.type.physicalSize }.sum()).toFloat()
            )
        } else {
            Cost.INVALID
        }

    /**
     * Checks if the provided [Predicate] can be processed by this instance of [VAFIndex].
     *
     * @param predicate The [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate) = predicate is KnnPredicate && predicate.column == this.columns[0]

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [VAFIndex].
     *
     * @param context The [TransactionContext] to create this [IndexTx] for.
     */
    override fun newTx(context: TransactionContext): IndexTx = Tx(context)

    /**
     * A [IndexTx] that affects this [AbstractIndex].
     */
    private inner class Tx(context: TransactionContext) : AbstractIndex.Tx(context) {
        /**
         * Returns the number of [VAFSignature]s in this [VAFIndex]
         *
         * @return The number of [VAFSignature] stored in this [VAFIndex]
         */
        override fun count(): Long = this.withReadLock {
            this@VAFIndex.signatures.size.toLong()
        }

        /**
         * (Re-)builds the [VAFIndex] from scratch.
         */
        override fun rebuild() = this.withWriteLock {
            LOGGER.debug("Rebuilding VAF index {}", this@VAFIndex.name)

            /* Obtain transaction and calculate maximum per dimension.. */
            val txn = this.context.getTx(this@VAFIndex.parent) as EntityTx
            val min = DoubleArray(this@VAFIndex.columns[0].type.logicalSize)
            val max = DoubleArray(this@VAFIndex.columns[0].type.logicalSize)
            txn.scan(this@VAFIndex.columns).forEach { r ->
                val value = r[this@VAFIndex.columns[0]] as VectorValue<*>
                for (i in 0 until value.logicalSize) {
                    min[i] = min(min[i], value[i].asDouble().value)
                    max[i] = max(max[i], value[i].asDouble().value)
                }
            }

            /* Calculate and update marks. */
            val marks = MarksGenerator.getEquidistantMarks(min, max, IntArray(this@VAFIndex.columns[0].type.logicalSize) { this@VAFIndex.config.marksPerDimension })
            this@VAFIndex.marksStore.set(marks)

            /* Calculate and update signatures. */
            this@VAFIndex.signatures.clear()
            txn.scan(this@VAFIndex.columns).forEach { r ->
                val value = r[this@VAFIndex.columns[0]]
                if (value is RealVectorValue<*>) {
                    this@VAFIndex.signatures.add(VAFSignature(r.tupleId, marks.getCells(value)))
                }
            }

            this@VAFIndex.dirtyField.compareAndSet(true, false)
            LOGGER.debug("Done rebuilding VAF index {}", this@VAFIndex.name)
        }

        /**
         * Updates the [VAFIndex] with the provided [DataChangeEvent]s. Since the [VAFIndex] does
         * not support incremental updates, calling this method will simply set the [VAFIndex]
         * [dirty] flag to true.
         *
         * @param event [DataChangeEvent]s to process.
         */
        override fun update(event: DataChangeEvent) = this.withWriteLock {
            this@VAFIndex.dirtyField.compareAndSet(false, true)
            Unit
        }

        /**
         * Performs a lookup through this [VAFIndex.Tx] and returns a [Iterator] of all [Record]s
         * that match the [Predicate]. Only supports [KnnPredicate]s.
         *
         * <strong>Important:</strong> The [Iterator] is not thread safe! It remains to the
         * caller to close the [Iterator]
         *
         * @param predicate The [Predicate] for the lookup
         * @return The resulting [Iterator]
         */
        override fun filter(predicate: Predicate) = filterRange(predicate, 0L until this.count())

        /**
         * Performs a lookup through this [VAFIndex.Tx] and returns a [Iterator] of all [Record]s
         * that match the [Predicate] within the given [LongRange]. Only supports [KnnPredicate]s.
         *
         * <strong>Important:</strong> The [Iterator] is not thread safe!
         *
         * @param predicate The [Predicate] for the lookup
         * @param range The [LongRange] of [VAFSignature]s to consider.
         * @return The resulting [Iterator]
         */
        override fun filterRange(predicate: Predicate, range: LongRange) =
            object : Iterator<Record> {

                /** Cast  to [KnnPredicate] (if such a cast is possible).  */
                private val predicate = if (predicate is KnnPredicate) {
                    predicate
                } else {
                    throw QueryException.UnsupportedPredicateException("Index '${this@VAFIndex.name}' (VAF Index) does not support predicates of type '${predicate::class.simpleName}'.")
                }

                /** The [Marks] used by this [Iterator]. */
                private val marks = this@VAFIndex.marksStore.get()

            /** The [Bounds] objects used for filtering. */
            private val bounds: List<Bounds> = this.predicate.query.map {
                require(it is RealVectorValue<*>) { }
                when (this.predicate.distance) {
                    is ManhattanDistance -> L1Bounds(it, this.marks)
                    is EuclidianDistance -> L2Bounds(it, this.marks)
                    is SquaredEuclidianDistance -> L2SBounds(it, this.marks)
                    is MinkowskiDistance -> LpBounds(it, this.marks, this.predicate.distance.p)
                    else -> throw IllegalArgumentException("The ${this.predicate.distance} distance kernel is not supported by VAFIndex.")
                }
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

                /* Prepare txn and kNN data structures. */
                val txn = this@Tx.context.getTx(this@VAFIndex.parent) as EntityTx
                val knns = if (this.predicate.k == 1) {
                    this.predicate.query.map { MinSingleSelection<ComparablePair<Long, DoubleValue>>() }
                } else {
                    this.predicate.query.map {
                        MinHeapSelection<ComparablePair<Long, DoubleValue>>(
                            this.predicate.k
                        )
                    }
                }

                /* Iterate over all signatures. */
                var read = 0L
                for (sigIndex in range) {
                    val signature = this@VAFIndex.signatures[sigIndex.toInt()]
                    if (signature != null) {
                        this.predicate.query.forEachIndexed { i, q ->
                            if (q is RealVectorValue<*>) {
                                if (knns[i].size < this.predicate.k || this.bounds[i].isVASSACandidate(
                                        signature,
                                        knns[i].peek()!!.second.value
                                    )
                                ) {
                                    val value = txn.read(
                                        signature.tupleId,
                                        this@VAFIndex.columns
                                    )[this@VAFIndex.columns[0]]
                                    if (value is VectorValue<*>) {
                                        knns[i].offer(
                                            ComparablePair(
                                                signature.tupleId,
                                                this.predicate.distance(value, q)
                                            )
                                        )
                                    }
                                    read += 1
                                }
                            }
                        }
                    }
                }

                val skipped = ((1.0 - (read.toDouble() / (this@VAFIndex.signatures.size))) * 100)
                LOGGER.debug("VA-file scan: Skipped over $skipped% of entries.")

                /* Prepare and return list of results. */
                val queue =
                    ArrayDeque<StandaloneRecord>(this.predicate.k * this.predicate.query.size)
                for ((queryIndex, knn) in knns.withIndex()) {
                    for (i in 0 until knn.size) {
                        queue.add(
                            StandaloneRecord(
                                knn[i].first,
                                this@VAFIndex.produces,
                                arrayOf(IntValue(queryIndex), knn[i].second)
                            )
                        )
                    }
                }
                return queue
            }
        }
    }
}