package org.vitrivr.cottontail.database.index.va

import org.mapdb.Atomic
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.database.entity.Entity
import org.vitrivr.cottontail.database.entity.EntityTx
import org.vitrivr.cottontail.database.events.DataChangeEvent
import org.vitrivr.cottontail.database.index.Index
import org.vitrivr.cottontail.database.index.IndexTx
import org.vitrivr.cottontail.database.index.IndexType
import org.vitrivr.cottontail.database.index.va.bounds.Bounds
import org.vitrivr.cottontail.database.index.va.bounds.L1Bounds
import org.vitrivr.cottontail.database.index.va.bounds.L2Bounds
import org.vitrivr.cottontail.database.index.va.bounds.LpBounds
import org.vitrivr.cottontail.database.index.va.signature.Marks
import org.vitrivr.cottontail.database.index.va.signature.MarksGenerator
import org.vitrivr.cottontail.database.index.va.signature.Signature
import org.vitrivr.cottontail.database.queries.components.AtomicBooleanPredicate
import org.vitrivr.cottontail.database.queries.components.KnnPredicate
import org.vitrivr.cottontail.database.queries.components.Predicate
import org.vitrivr.cottontail.database.queries.planning.cost.Cost
import org.vitrivr.cottontail.execution.TransactionContext
import org.vitrivr.cottontail.math.knn.metrics.EuclidianDistance
import org.vitrivr.cottontail.math.knn.metrics.ManhattanDistance
import org.vitrivr.cottontail.math.knn.metrics.MinkowskiDistance
import org.vitrivr.cottontail.math.knn.selection.ComparablePair
import org.vitrivr.cottontail.math.knn.selection.MinHeapSelection
import org.vitrivr.cottontail.math.knn.selection.MinSingleSelection
import org.vitrivr.cottontail.model.basics.CloseableIterator
import org.vitrivr.cottontail.model.basics.ColumnDef
import org.vitrivr.cottontail.model.basics.Name
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
import kotlin.time.ExperimentalTime
import kotlin.time.measureTimedValue

/**
 * An vector approximation (VA) file based [Index] that can be used for optimized nearest neighbor
 * search using [Bounds]. Based on the algorithm proposed in [1].
 *
 * References:
 * [1] Weber, R. and Blott, S., 1997. An approximation based data structure for similarity search (No. 9141, p. 416). Technical Report 24, ESPRIT Project HERMES.
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 1.0.1
 */
class VAFIndex(override val name: Name.IndexName, override val parent: Entity, override val columns: Array<ColumnDef<*>>, config: VAFIndexConfig? = null) : Index() {

    companion object {
        private const val CONFIG_NAME = "vaf_config"
        private const val SIGNATURE_FIELD_NAME = "vaf_signatures"
        private const val MARKS_FIELD_NAME = "vaf_marks"
        val LOGGER: Logger = LoggerFactory.getLogger(VAFIndex::class.java)
    }

    /** The [Path] to the [VAFIndex]'s main file OR folder. */
    override val path: Path = this.parent.path.resolve("idx_vaf_$name.db")

    /** The [VAFIndex] implementation returns exactly the columns that is indexed. */
    override val produces: Array<ColumnDef<*>> = arrayOf(
            KnnUtilities.queryIndexColumnDef(this.parent.name),
            KnnUtilities.distanceColumnDef(this.parent.name)
    )

    /** The type of [Index]. */
    override val type = IndexType.VAF

    /** The internal [DB] reference. */
    private val db = this.parent.parent.parent.config.mapdb.db(this.path)

    /** The [VAFIndexConfig] used by this [VAFIndex] instance. */
    private val config: VAFIndexConfig

    /** Store for the [Marks]. */
    private val marksStore: Atomic.Var<Marks> = this.db.atomicVar(MARKS_FIELD_NAME, Marks.Serializer).createOrOpen()

    /** Store for the signatures. */
    private val signatures = this.db.indexTreeList(SIGNATURE_FIELD_NAME, Signature.Serializer).createOrOpen()

    init {
        require(this.columns.size == 1) { "$VAFIndex only supports indexing a single column." }

        /* Load or create config. */
        val configOnDisk = this.db.atomicVar(CONFIG_NAME, VAFIndexConfig.Serializer).createOrOpen()
        if (configOnDisk.get() == null) {
            if (config != null) {
                this.config = config
            } else {
                this.config = VAFIndexConfig(5)
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
     * TODO: Calculate actual cost.
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate) = Cost.ZERO

    /**
     * Checks if the provided [Predicate] can be processed by this instance of [VAFIndex].
     *
     * @param predicate The [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate) = predicate is KnnPredicate<*> && predicate.column == this.columns[0]


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
         * (Re-)builds the [VAFIndex] from scratch.
         */
        override fun rebuild() = this.withWriteLock {
            LOGGER.trace("Rebuilding VAF index {}", this@VAFIndex.name)

            /* Obtain transaction and calculate maximum per dimension.. */
            val txn = this.context.getTx(this@VAFIndex.parent) as EntityTx
            val min = DoubleArray(this@VAFIndex.columns[0].logicalSize)
            val max = DoubleArray(this@VAFIndex.columns[0].logicalSize)
            txn.scan(this@VAFIndex.columns).forEach { r ->
                val value = r[this@VAFIndex.columns[0]] as VectorValue<*>
                for (i in 0 until value.logicalSize) {
                    min[i] = min(min[i], value[i].asDouble().value)
                    max[i] = max(max[i], value[i].asDouble().value)
                }
            }

            /* Calculate and update marks. */
            val marks = MarksGenerator.getEquidistantMarks(min, max, IntArray(this@VAFIndex.columns[0].logicalSize) { this@VAFIndex.config.marksPerDimension })
            this@VAFIndex.marksStore.set(marks)

            /* Calculate and update signatures. */
            this@VAFIndex.signatures.clear()
            txn.scan(this@VAFIndex.columns).forEach { r ->
                val value = r[this@VAFIndex.columns[0]]
                if (value is RealVectorValue<*>) {
                    this@VAFIndex.signatures.add(Signature(r.tupleId, marks.getCells(value)))
                }
            }

            LOGGER.trace("Done rebuilding VAF index {}", this@VAFIndex.name)
        }

        /**
         * Updates the [VAFIndex] with the provided [DataChangeEvent]s. This method determines, whether the [Record]
         * affected by the [DataChangeEvent] should be added or updated
         *
         * @param update Collection of [DataChangeEvent]s to process.
         */
        override fun update(update: Collection<DataChangeEvent>) = this.withWriteLock {
            TODO("Not yet implemented")
        }

        /**
         * Performs a lookup through this [VAFIndex.Tx] and returns a [CloseableIterator] of all [Record]s
         * that match the [Predicate]. Only supports [KnnPredicate]s.
         *
         * <strong>Important:</strong> The [CloseableIterator] is not thread safe! It remains to the
         * caller to close the [CloseableIterator]
         *
         * @param predicate The [Predicate] for the lookup
         * @return The resulting [CloseableIterator]
         */
        @ExperimentalTime
        override fun filter(predicate: Predicate): CloseableIterator<Record> = object : CloseableIterator<Record> {

            /** Cast [AtomicBooleanPredicate] (if such a cast is possible).  */
            private val predicate = if (predicate is KnnPredicate<*>) {
                predicate
            } else {
                throw QueryException.UnsupportedPredicateException("Index '${this@VAFIndex.name}' (VAF Index) does not support predicates of type '${predicate::class.simpleName}'.")
            }

            /** The [Marks] used by this [CloseableIterator]. */
            private val marks = this@VAFIndex.marksStore.get()

            /** The [Bounds] objects used for filtering. */
            private val bounds: List<Bounds> = this.predicate.query.map {
                require(it is RealVectorValue<*>) { }
                when (this.predicate.distance) {
                    is ManhattanDistance -> L1Bounds(it, this.marks)
                    is EuclidianDistance -> L2Bounds(it, this.marks)
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

            /** Flag indicating whether this [CloseableIterator] has been closed. */
            @Volatile
            private var closed = false

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

                /* Prepare txn and kNN data structures. */
                val txn = this@Tx.context.getTx(this@VAFIndex.parent) as EntityTx
                val knns = if (this.predicate.k == 1) {
                    this.predicate.query.map { MinSingleSelection<ComparablePair<Long, DoubleValue>>() }
                } else {
                    this.predicate.query.map { MinHeapSelection<ComparablePair<Long, DoubleValue>>(this.predicate.k) }
                }

                var read = 0L
                var skipped = 0L

                /* Iterate over all signatures. */
                val time = measureTimedValue {
                    for (signature in this@VAFIndex.signatures) {
                        if (signature != null) {
                            this.predicate.query.forEachIndexed { i, q ->
                                if (q is RealVectorValue<*>) {
                                    if (knns[i].size < this.predicate.k || knns[i].peek()!!.second.value > this.bounds[i].update(signature).lb) {
                                        val value = txn.read(signature.tupleId, this@VAFIndex.columns)[this@VAFIndex.columns[0]]
                                        if (value is VectorValue<*>) {
                                            knns[i].offer(ComparablePair(signature.tupleId, this.predicate.distance(value, q)))
                                        }
                                        read += 1
                                    } else {
                                        skipped += 1
                                    }
                                }
                            }
                        }
                    }
                    (skipped.toDouble() / (read + skipped)) * 100
                }

                LOGGER.info("VA-file scan took ${time.duration}: Skipped over ${time.value}% of entries.")

                /* Prepare and return list of results. */
                val queue = ArrayDeque<StandaloneRecord>(this.predicate.k * this.predicate.query.size)
                for ((queryIndex, knn) in knns.withIndex()) {
                    for (i in 0 until knn.size) {
                        queue.add(StandaloneRecord(knn[i].first, this@VAFIndex.produces, arrayOf(IntValue(queryIndex), knn[i].second)))
                    }
                }
                return queue
            }
        }

        /**
         * Commits changes and update signature cache.
         */
        override fun performCommit() {
            this@VAFIndex.db.commit()
        }

        /**
         * Makes a rollback on all changes to the index.
         */
        override fun performRollback() {
            this@VAFIndex.db.rollback()
        }
    }
}