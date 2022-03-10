package org.vitrivr.cottontail.dbms.index.gg

import org.slf4j.LoggerFactory
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.functions.Argument
import org.vitrivr.cottontail.core.queries.functions.Signature
import org.vitrivr.cottontail.core.queries.functions.math.VectorDistance
import org.vitrivr.cottontail.core.queries.planning.cost.Cost
import org.vitrivr.cottontail.core.queries.predicates.Predicate
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.VectorValue
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexCatalogueEntry
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.exceptions.QueryException
import org.vitrivr.cottontail.dbms.execution.TransactionContext
import org.vitrivr.cottontail.dbms.index.*
import org.vitrivr.cottontail.dbms.index.basics.avc.AuxiliaryValueCollection
import org.vitrivr.cottontail.dbms.index.pq.PQIndex
import org.vitrivr.cottontail.dbms.operations.Operation
import org.vitrivr.cottontail.utilities.math.KnnUtilities
import org.vitrivr.cottontail.utilities.selection.ComparablePair
import org.vitrivr.cottontail.utilities.selection.MinHeapSelection
import org.vitrivr.cottontail.utilities.selection.MinSingleSelection
import java.util.*
import kotlin.collections.ArrayDeque
import kotlin.concurrent.withLock

/**
 * An index structure for nearest neighbour search (NNS) based on fast grouping algorithm proposed in [1].
 *
 * Can be used for all types of [VectorValue]s (real and complex) as well as [VectorDistance]s. However, the index
 * must be built and prepared for a specific [VectorDistance].
 *
 * References:
 * [1] Cauley, Stephen F., et al. "Fast group matching for MR fingerprinting reconstruction." Magnetic resonance in medicine 74.2 (2015): 523-528.
 *
 * @author Gabriel Zihlmann & Ralph Gasser
 * @version 3.0.0
 */
class GGIndex(name: Name.IndexName, parent: DefaultEntity) : AbstractHDIndex(name, parent) {
    companion object {
        val LOGGER = LoggerFactory.getLogger(GGIndex::class.java)!!
    }

    /** The [PQIndex] implementation returns exactly the columns that is indexed. */
    override val produces: Array<ColumnDef<*>> = arrayOf(KnnUtilities.distanceColumnDef(this.parent.name))

    /** The type of [AbstractIndex]. */
    override val type = IndexType.GG

    /** The [GGIndexConfig] used by this [GGIndex] instance. */
    override val config: GGIndexConfig = this.catalogue.environment.computeInTransaction { tx ->
        val entry = IndexCatalogueEntry.read(this.name, this.parent.parent.parent, tx) ?: throw DatabaseException.DataCorruptionException("Failed to read catalogue entry for index ${this.name}.")
        GGIndexConfig.fromParamsMap(entry.config)
    }

    /** False since [GGIndex] doesn't support incremental updates. */
    override val supportsIncrementalUpdate: Boolean = false

    /** True since [GGIndex] does not support partitioning. */
    override val supportsPartitioning: Boolean = false

    init {
        require(this.columns.size == 1) { "GGIndex only supports indexing a single column." }
        require(this.columns[0].type is Types.Vector<*,*>) { "GGIndex only supports indexing of vector columns." }
    }

    /**
     * Checks if this [AbstractIndex] can process the provided [Predicate] and returns true if so and false otherwise.
     *
     * @param predicate [Predicate] to check.
     * @return True if [Predicate] can be processed, false otherwise.
     */
    override fun canProcess(predicate: Predicate) = predicate is ProximityPredicate
            && predicate.column == this.columns[0]
            && predicate.distance.signature.name == this.config.distance.functionName

    /**
     * Calculates the cost estimate if this [GGIndex] processing the provided [Predicate].
     *
     * @param predicate [Predicate] to check.
     * @return Cost estimate for the [Predicate]
     */
    override fun cost(predicate: Predicate) = Cost.ZERO // todo...

    /**
     * Opens and returns a new [IndexTx] object that can be used to interact with this [GGIndex].
     *
     * @param context The [TransactionContext] to create this [IndexTx] for.
     */
    override fun newTx(context: TransactionContext): IndexTx = Tx(context)

    /**
     * Closes this [GGIndex]
     */
    override fun close() {
        /* No op. */
    }

    /**
     * A [IndexTx] that affects this [GGIndex].
     */
    private inner class Tx(context: TransactionContext) : AbstractHDIndex.Tx(context) {

        /** The [GGIndexConfig] used by this [GGIndex] instance. */
        override val config: GGIndexConfig
            get() {
                val entry = IndexCatalogueEntry.read(this@GGIndex.name, this@GGIndex.parent.parent.parent, this.context.xodusTx) ?: throw DatabaseException.DataCorruptionException("Failed to read catalogue entry for index ${this@GGIndex.name}.")
                return GGIndexConfig.fromParamsMap(entry.config)
            }

        /**
         * Rebuilds the surrounding [PQIndex] from scratch using the following, greedy grouping algorithm:
         *
         *  # Takes one dictionary element (random is probably easiest to start with)
         *  # Go through all yet ungrouped elements and find k = groupSize = numElementsTotal/numGroups most similar ones
         *  # Build mean vector of those k in the group and store as group representation
         *  # Don't do any PCA/SVD as we only have 18-25 ish dims...
         *  # Repeat with a new randomly selected element from the remaining ones until no elements remain.
         *
         *  Takes around 6h for 5000 groups on 9M vectors
         */
        override fun rebuild() = this.txLatch.withLock {

            /* Obtain some learning data for training. */
            PQIndex.LOGGER.debug("Rebuilding GG index {}", this@GGIndex.name)

            /* Load all tuple ids into a set. */
            val txn = this.context.getTx(this.dbo.parent) as EntityTx
            val remainingTids = mutableSetOf<Long>()
            txn.cursor(emptyArray()).forEach { remainingTids.add(it.tupleId) }

            /* Prepare necessary data structures. */
            val groupSize =
                ((remainingTids.size + this.config.numGroups - 1) / this.config.numGroups)  // ceildiv
            val finishedTIds = mutableSetOf<Long>()
            val random = SplittableRandom(this.config.seed)

            /* Start rebuilding the index. */
            this.clear()
            while (remainingTids.isNotEmpty()) {
                /* Randomly pick group seed value. */
                val groupSeedTid = remainingTids.elementAt(random.nextInt(remainingTids.size))
                val groupSeedValue = txn.read(groupSeedTid, this.columns)[this.columns[0]]
                if (groupSeedValue is VectorValue<*>) {
                    /* Perform kNN for group. */
                    val signature = Signature.Closed(this.config.distance.functionName, arrayOf(Argument.Typed(this.columns[0].type)), Types.Double)
                    val function = this@GGIndex.parent.parent.parent.functions.obtain(signature)
                    check(function is VectorDistance<*>) { "GGIndex rebuild failed: Function $signature is not a vector distance function." }
                    val knn = MinHeapSelection<ComparablePair<Pair<TupleId, VectorValue<*>>, DoubleValue>>(groupSize)
                    remainingTids.forEach { tid ->
                        val r = txn.read(tid, this.columns)
                        val vec = r[this.columns[0]]
                        if (vec is VectorValue<*>) {
                            val distance = function(vec)
                            if (distance != null && (knn.size < groupSize || knn.peek()!!.second > distance)) {
                                knn.offer(ComparablePair(Pair(tid, vec), distance))
                            }
                        }
                    }

                    var groupMean = groupSeedValue.new()
                    val groupTids = mutableListOf<Long>()
                    for (i in 0 until knn.size) {
                        val element = knn[i].first
                        groupMean += element.second
                        groupTids.add(element.first)
                        check(remainingTids.remove(element.first)) { "${name.simple} processed an element that should have been removed by now." }
                        check(finishedTIds.add(element.first)) { "${name.simple} processed an element that was already processed." }
                    }
                    groupMean /= DoubleValue(knn.size)
                    //TODO: this@GGIndex.groupsStore[groupMean] = groupTids.toLongArray()
                }
            }
            this.updateState(IndexState.CLEAN)
            PQIndex.LOGGER.debug("Rebuilding GGIndex {} complete.", this@GGIndex.name)
        }

        override val auxiliary: AuxiliaryValueCollection
            get() = TODO("Not yet implemented")
        /**
         * Clears the [GGIndex] underlying this [Tx] and removes all entries it contains.
         */
        override fun clear() = this.txLatch.withLock {
            this.updateState(IndexState.STALE)
            //TODO: this@GGIndex.groupsStore.clear()
        }

        /**
         * Performs a lookup through this [PQIndex.Tx] and returns a [Cursor] of all [Record]s that match the [Predicate].
         * Only supports [ProximityPredicate]s.
         *
         * <strong>Important:</strong> The [Cursor] is not thread safe! It remains to the caller to close the [Cursor]
         *
         * @param predicate The [Predicate] for the lookup
         * @return The resulting [Iterator]
         */
        override fun filter(predicate: Predicate): Cursor<Record> = object : Cursor<Record> {

            /** Cast [ProximityPredicate] (if such a cast is possible).  */
            private val predicate = if (predicate is ProximityPredicate && predicate.distance.signature.name == this@Tx.config.distance.functionName) {
                predicate
            } else {
                throw QueryException.UnsupportedPredicateException("Index '${this@GGIndex.name}' (GGIndex) does not support predicates of type '${predicate::class.simpleName}'.")
            }

            /** List of query [VectorValue]s. Must be prepared before using the [Iterator]. */
            private val vector: VectorValue<*>

            /** The [ArrayDeque] of [StandaloneRecord] produced by this [GGIndex]. Evaluated lazily! */
            private val resultsQueue: ArrayDeque<StandaloneRecord> by lazy { prepareResults() }

            init {
                val value = this.predicate.query.value
                check(value is VectorValue<*>) { "Bound value for query vector has wrong type (found = ${value?.type})." }
                this.vector = value
            }

            override fun moveNext(): Boolean = if (this.resultsQueue.isNotEmpty()) {
                this.resultsQueue.removeFirst()
                true
            } else {
                false
            }

            override fun key(): TupleId = this.resultsQueue.first().tupleId

            override fun value(): Record = this.resultsQueue.first()

            override fun close() {
                this.resultsQueue.clear()
            }

            /**
             * Executes the kNN and prepares the results to return by this [Iterator].
             */
            private fun prepareResults(): ArrayDeque<StandaloneRecord> {
                /* Scan >= 10% of entries by default */
                val considerNumGroups = (this@Tx.config.numGroups + 9) / 10
                val txn = this@Tx.context.getTx(this@GGIndex.parent) as EntityTx
                val signature = Signature.Closed(this@Tx.config.distance.functionName, arrayOf(Argument.Typed(this@Tx.columns[0].type)), Types.Double)
                val function = this@GGIndex.parent.parent.parent.functions.obtain(signature)
                check (function is VectorDistance<*>) { "Function $signature is not a vector distance function." }

                /** Phase 1): Perform kNN on the groups. */
                require(this.predicate.k < txn.maxTupleId() / this@Tx.config.numGroups * considerNumGroups) { "Value of k is too large for this index considering $considerNumGroups groups." }
                val groupKnn = MinHeapSelection<ComparablePair<LongArray, DoubleValue>>(considerNumGroups)

                LOGGER.debug("Scanning group mean signals.")
                //TODO: this@GGIndex.groupsStore.forEach {
                //    groupKnn.offer(ComparablePair(it.value, function(it.key)))
                //}


                /** Phase 2): Perform kNN on the per-group results. */
                val knn = if (this.predicate.k == 1) {
                    MinSingleSelection<ComparablePair<Long, DoubleValue>>()
                } else {
                    MinHeapSelection(this.predicate.k)
                }
                LOGGER.debug("Scanning group members.")
                for (k in 0 until groupKnn.size) {
                    for (tupleId in groupKnn[k].first) {
                        val value =
                            txn.read(tupleId, this@Tx.columns)[this@Tx.columns[0]]
                        if (value is VectorValue<*>) {
                            val distance = function(value)
                            if (distance != null && (knn.size < knn.k || knn.peek()!!.second > distance)) {
                                knn.offer(ComparablePair(tupleId, distance))
                            }
                        }
                    }
                }

                /* Phase 3: Prepare and return list of results. */
                val queue = ArrayDeque<StandaloneRecord>(this.predicate.k)
                for (i in 0 until knn.size) {
                    queue.add(StandaloneRecord(knn[i].first, this@GGIndex.produces, arrayOf(knn[i].second)))
                }
                return queue
            }
        }

        /**
         * Range filtering is not supported [GGIndex]
         *
         * @param predicate The [Predicate] for the lookup.
         * @param partitionIndex The [partitionIndex] for this [filterRange] call.
         * @param partitions The total number of partitions for this [filterRange] call.
         * @return The resulting [Cursor].
         */
        override fun filterRange(predicate: Predicate, partitionIndex: Int, partitions: Int): Cursor<Record> {
            throw UnsupportedOperationException("The UniqueHashIndex does not support ranged filtering!")
        }

        /**
         * The [GGIndex] does not support incremental updates. Hence, this method will always throw an [UnsupportedOperationException].
         */
        override fun tryApply(event: Operation.DataManagementOperation.InsertOperation): Boolean {
            throw UnsupportedOperationException("GGIndex does not support incremental updates!")
        }

        /**
         * The [GGIndex] does not support incremental updates. Hence, this method will always throw an [UnsupportedOperationException].
         */
        override fun tryApply(event: Operation.DataManagementOperation.UpdateOperation): Boolean {
            throw UnsupportedOperationException("GGIndex does not support incremental updates!")
        }

        /**
         * The [GGIndex] does not support incremental updates. Hence, this method will always throw an [UnsupportedOperationException].
         */
        override fun tryApply(event: Operation.DataManagementOperation.DeleteOperation): Boolean{
            throw UnsupportedOperationException("GGIndex does not support incremental updates!")
        }
    }
}