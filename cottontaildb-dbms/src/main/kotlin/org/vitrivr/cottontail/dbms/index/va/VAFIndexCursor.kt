package org.vitrivr.cottontail.dbms.index.va

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.binding.MissingTuple
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.ManhattanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.euclidean.EuclideanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.squaredeuclidean.SquaredEuclideanDistance
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.RealVectorValue
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.dbms.catalogue.entries.IndexStructuralMetadata
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.operators.sort.RecordComparator
import org.vitrivr.cottontail.dbms.index.basic.IndexMetadata.Companion.storeName
import org.vitrivr.cottontail.dbms.index.va.bounds.Bounds
import org.vitrivr.cottontail.dbms.index.va.bounds.L1Bounds
import org.vitrivr.cottontail.dbms.index.va.bounds.L2Bounds
import org.vitrivr.cottontail.dbms.index.va.signature.EquidistantVAFMarks
import org.vitrivr.cottontail.dbms.index.va.signature.VAFSignature
import org.vitrivr.cottontail.utilities.selection.HeapSelection
import java.util.concurrent.atomic.AtomicBoolean

/**
 * A [Cursor] implementation for the [VAFIndex].
 *
 * @author Ralph Gasser
 * @version 1.3.0
 */
@Suppress("UNCHECKED_CAST")
sealed class VAFIndexCursor<T: ProximityPredicate>(protected val partition: LongRange, protected val predicate: T, protected val index: VAFIndex.Tx): Cursor<Tuple> {
    /** [VectorValue] used for query. Must be prepared before using the [Iterator]. */
    protected val query: RealVectorValue<*>

    /** The [Bounds] objects used for filtering. */
    protected val bounds: Bounds

    /** The sub-transaction used by this [VAFIndexCursor]. */
    private val xodusTx = index.xodusTx.readonlySnapshot

    /** The Xodus [Store] used to store [VAFSignature]s. */
    private val store: Store = this.xodusTx.environment.openStore(index.dbo.name.storeName(), StoreConfig.USE_EXISTING, this.xodusTx, false)
        ?: throw DatabaseException.DataCorruptionException("Store for VAF index ${index.dbo.name} is missing.")

    /** Cursor backing this [VAFIndexCursor]. */
    protected val cursor: jetbrains.exodus.env.Cursor = this.store.openCursor(this.xodusTx)

    /** A beginning of cursor (BoC) flag. */
    protected val boc = AtomicBoolean(true)

    /** The [TupleId] to start with. */
    private val startKey = this.partition.first.toKey()

    /** The [TupleId] to end at. */
    protected val endKey = this.partition.last.toKey()

    /** Cached in-memory version of the [EquidistantVAFMarks] used by this [Cursor]. */
    protected val marks : EquidistantVAFMarks by lazy {
        IndexStructuralMetadata.read(this.index, EquidistantVAFMarks.Binding) ?: throw DatabaseException.DataCorruptionException("Marks for VAF index ${index.dbo.name} are missing.")
    }

    /** The columns produced by this [Cursor]. */
    protected val produces = index.columnsFor(this.predicate).toTypedArray()

    /** The [ColumnDef] of the column that contains the vector. */
    protected val valueColumn: ColumnDef<*> = this.index.columns[0]

    init {
        /* Extract query vector from binding. */
        val queryVectorBinding = this.predicate.query
        with(MissingTuple) {
            with(index.context.bindings) {
                val value = queryVectorBinding.getValue()
                check(value is RealVectorValue<*>) { "Bound value for query vector has wrong type (found = ${value?.type})." }
                this@VAFIndexCursor.query = value
            }
        }

        /* Derive bounds object. */
        this.bounds = when (this.predicate.distance) {
            is ManhattanDistance<*> -> L1Bounds(this.query, this.marks)
            is EuclideanDistance<*>,
            is SquaredEuclideanDistance<*> -> L2Bounds(this.query, this.marks)

            else -> throw IllegalArgumentException("The ${this.predicate.distance} distance kernel is not supported by VAFIndex.")
        }

        /* Move cursors to correct position. */
        if (this.cursor.getSearchKeyRange(this.startKey) == null) {
            this.boc.set(false)
        }
    }

    /**
     * Closes this [Cursor]
     */
    override fun close() {
        this.cursor.close()
        this.xodusTx.abort()
    }

    /**
     * Calculates, updates and reports [VAFIndex] efficiency.
     *
     * @param retrieved The number of entries that had to be retrieved.
     */
    protected fun reportAndUpdateEfficiency(retrieved: Long) {
        /* Log efficiency of VAF scan. */
        val partitionSize = this.partition.last - this.partition.first + 1
        val efficiency = (1.0f - (retrieved.toFloat() / partitionSize))
        this.index.updateEfficiency(efficiency)
        VAFIndex.LOGGER.debug("VA-SSA Scan: Read $retrieved and skipped over ${"%.2f".format(efficiency * 100.0f)}% of entries.")
    }

    /**
     * A [VAFIndexCursor] for limiting [ProximityPredicate], i.e., [ProximityPredicate.NNS] and [ProximityPredicate.FNS]
     */
    sealed class KLimited<T : ProximityPredicate.KLimitedSearch>(partition: LongRange, predicate: T, index: VAFIndex.Tx) : VAFIndexCursor<T>(partition, predicate, index) {

        /** The [Iterator] over the top k entries. */
        protected val selection: Iterator<Tuple> by lazy {
            if (this.boc.compareAndExchange(true, false)) {
                prepareVASSA().iterator()
            } else {
                emptyList<Tuple>().iterator()
            }
        }

        /** The current [Tuple] this [KLimited] is pointing to.  */
        protected var current: Tuple? = null

        /**
         * Moves the internal cursor and return true, as long as new candidates appear.
         */
        override fun moveNext(): Boolean {
            if (!this.selection.hasNext()) return false
            this.current = this.selection.next()
            return true
        }

        /**
         * Returns the current [TupleId] this [Cursor] is pointing to.
         *
         * @return [TupleId]
         */
        override fun key(): TupleId = try {
            this.current!!.tupleId
        } catch (e: NullPointerException) {
            throw IllegalStateException("VAFCursor is not currently pointing to a record.")
        }

        /**
         * Returns the current [Tuple] this [Cursor] is pointing to.
         *
         * @return [Tuple]
         */
        override fun value(): Tuple = try {
            this.current!!
        } catch (e: NullPointerException) {
            throw IllegalStateException("VAFCursor is not currently pointing to a record.")
        }

        /**
         * Prepares the [HeapSelection] for this [VAFIndexCursor.KLimited]
         *
         * @return [HeapSelection]
         */
        protected abstract fun prepareVASSA(): HeapSelection<Tuple>
    }

    /**
     * A [VAFIndexCursor] implementation for nearest neighbour search.
     */
    class NNS(partition: LongRange, predicate: ProximityPredicate.NNS, index: VAFIndex.Tx) : KLimited<ProximityPredicate.NNS>(partition, predicate, index) {

        /**
         * Prepares the result set using the [VAFIndex] and the VA-SSA algorithm described in [1].
         *
         * @return Prepared [HeapSelection]
         */
        override fun prepareVASSA(): HeapSelection<Tuple> {
            val localSelection = HeapSelection(this.predicate.k.toInt(), RecordComparator.SingleNonNullColumnComparator(this.predicate.distanceColumn.column, SortOrder.ASCENDING))
            try {
                /* First phase: Just add entries until we have k-results. */
                var threshold: Double
                do {
                    val tupleId = LongBinding.compressedEntryToLong(cursor.key)
                    val value = index.parent.read(tupleId)[this.valueColumn] as VectorValue<*>
                    val distance = this.predicate.distance(this.query, value)!!
                    localSelection.offer(StandaloneTuple(tupleId, this.produces, arrayOf(distance, value)))
                } while (localSelection.size < localSelection.k && this.cursor.next && this.cursor.key <= this.endKey)

                /* Second phase: Use lower-bound to decide whether entry should be added. */
                threshold = (localSelection.peek()!![0] as DoubleValue).value
                while (this.cursor.next && this.cursor.key <= this.endKey) {
                    val signature = VAFSignature.fromEntry(cursor.value)
                    if (this.bounds.lb(signature, threshold) < threshold) {
                        val tupleId = LongBinding.compressedEntryToLong(cursor.key)
                        val value = index.parent.read(tupleId)[this.valueColumn]  as VectorValue<*>
                        val distance = this.predicate.distance(this.query, value)!!
                        threshold = (localSelection.offer(StandaloneTuple(tupleId, this.produces, arrayOf(distance, value)))[0] as DoubleValue).value
                    }
                }

                /* Log efficiency of VAF scan. */
                this.reportAndUpdateEfficiency(localSelection.added)
            } catch (e: Throwable) {
                VAFIndex.LOGGER.error("VA-SSA Scan: Error while scanning VAF index: ${e.message}")
                e.printStackTrace()
            }
            return localSelection
        }
    }

    /**
     * A [VAFIndexCursor] implementation for farthest neighbour search.
     */
    class FNS(partition: LongRange, predicate: ProximityPredicate.FNS, index: VAFIndex.Tx) : KLimited<ProximityPredicate.FNS>(partition, predicate, index) {

        /**
         * Prepares the result set using the [VAFIndex] and the VA-SSA algorithm described in [1].
         *
         * @return Prepared [HeapSelection]
         */
        override fun prepareVASSA(): HeapSelection<Tuple> {
            val localSelection = HeapSelection(this.predicate.k.toInt(), RecordComparator.SingleNonNullColumnComparator(this.predicate.distanceColumn.column, SortOrder.DESCENDING))
            try {
                /* First phase: Just add entries until we have k-results. */
                var threshold: Double
                do {
                    val tupleId = LongBinding.compressedEntryToLong(cursor.key)
                    val value = index.parent.read(tupleId)[this.valueColumn] as VectorValue<*>
                    val distance = this.predicate.distance(this.query, value)!!
                    localSelection.offer(StandaloneTuple(tupleId, this.produces, arrayOf(distance, value)))
                } while (localSelection.size < localSelection.k && this.cursor.next && this.cursor.key <= this.endKey)

                /* Second phase: Use lower-bound to decide whether entry should be added. */
                threshold = (localSelection.peek()!![0] as DoubleValue).value
                while (this.cursor.next && this.cursor.key <= this.endKey) {
                    val signature = VAFSignature.fromEntry(cursor.value)
                    if (this.bounds.ub(signature, threshold) < threshold) {
                        val tupleId = LongBinding.compressedEntryToLong(cursor.key)
                        val value = index.parent.read(tupleId)[this.valueColumn] as VectorValue<*>
                        val distance = this.predicate.distance(this.query, value)!!
                        threshold = (localSelection.offer(StandaloneTuple(tupleId, this.produces, arrayOf(distance, value)))[0] as DoubleValue).value
                    }
                }

                /* Log efficiency of VAF scan. */
                this.reportAndUpdateEfficiency(localSelection.added)
            } catch (e: Throwable) {
                VAFIndex.LOGGER.error("VA-SSA Scan: Error while scanning VAF index: ${e.message}")
                e.printStackTrace()
            }
            return localSelection
        }
    }

    /**
     * An (experimental) [VAFIndexCursor] implementation for range search.
     */
    class ENN(partition: LongRange, predicate: ProximityPredicate.ENN, index: VAFIndex.Tx) : VAFIndexCursor<ProximityPredicate.ENN>(partition, predicate, index) {

        override fun moveNext(): Boolean {
            while (this.boc.compareAndExchange(true, false) || (this.cursor.next && this.cursor.key < this.endKey)) {
                val signature = VAFSignature.fromEntry(this.cursor.value)
                val (lb,ub) = this.bounds.bounds(signature)
                if (this.predicate.eMin.value >= lb && this.predicate.eMax.value < ub) {
                    return true
                }
            }
            return false
        }

        override fun key(): TupleId = LongBinding.compressedEntryToLong(this.cursor.key)
        override fun value(): Tuple {
            val tupleId = LongBinding.compressedEntryToLong(this.cursor.key)
            val value = index.parent.read(tupleId)[this.valueColumn] as VectorValue<*>
            val distance = this.predicate.distance(this.query, value)!!
            return StandaloneTuple(tupleId, this.produces, arrayOf(distance, value))
        }
    }
}