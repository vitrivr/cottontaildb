package org.vitrivr.cottontail.dbms.index.va

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.binding.MissingTuple
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.EuclideanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.ManhattanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.SquaredEuclideanDistance
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.RealVectorValue
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.column.ColumnTx
import org.vitrivr.cottontail.dbms.execution.operators.sort.RecordComparator
import org.vitrivr.cottontail.dbms.execution.transactions.AccessMode
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
 * @version 1.2.0
 */
sealed class VAFCursor<T: ProximityPredicate>(tx: VAFIndex.Tx, context: BindingContext, protected val partition: LongRange, protected val predicate: T): Cursor<Tuple> {
    /** The [Bounds] objects used for filtering. */
    protected val bounds: Bounds

    /** The sub-transaction used by this [VAFCursor]. */
    private val xodusTx: Transaction = tx.xodusTx.readonlySnapshot

    /** The store containing the [VAFIndex] entries. */
    private val store: Store = this.xodusTx.environment.openStore(tx.dbo.name.storeName(), StoreConfig.WITHOUT_DUPLICATES, this.xodusTx)

    /** Cursor backing this [VAFCursor]. */
    protected val cursor: jetbrains.exodus.env.Cursor = this.store.openCursor(this.xodusTx)

    /** Begin of cursor (BoC) flag. */
    protected val boc = AtomicBoolean(true)

    /** Internal [ColumnTx] used to access actual values. */
    protected val columnTx: ColumnTx<*>

    /** The [TupleId] to start with. */
    protected val startKey = this.partition.first.toKey()

    /** The [TupleId] to end at. */
    protected val endKey = this.partition.last.toKey()

    /** Cached in-memory version of the [EquidistantVAFMarks] used by this [Cursor]. */
    protected val marks = tx.readMarks()

    /** The columns produced by this [Cursor]. */
    protected val produces = tx.columnsFor(this.predicate).toTypedArray()

    /** The query vector. */
    protected val query: RealVectorValue<*>

    init {
        /* Obtain query vector. Requires a binding context! */
        this.query = with(context) {
            with(MissingTuple) {
               this@VAFCursor.predicate.query.getValue() as? RealVectorValue<*> ?: throw IllegalArgumentException("The query vector for a VAFIndex must be a RealVectorValue. This is a progarmmer's error!")
            }
        }

        /* Derive bounds object. */
        this.bounds = when (this.predicate.distance) {
            is ManhattanDistance<*> -> L1Bounds(this.query, this.marks)
            is EuclideanDistance<*>,
            is SquaredEuclideanDistance<*> -> L2Bounds(this.query, this.marks)
            else -> throw IllegalArgumentException("The ${this.predicate.distance} distance kernel is not supported by VAFIndex.")
        }

        /* Obtain Tx object for column. */
        this.columnTx = tx.transaction.columnTx(this.predicate.column.name, AccessMode.READ)

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
        //TODO: this.index.updateEfficiency(efficiency)
        VAFIndex.LOGGER.debug("VA-SSA Scan: Read $retrieved and skipped over ${"%.2f".format(efficiency * 100.0f)}% of entries.")
    }

    /**
     * A [VAFCursor] for limiting [ProximityPredicate], i.e., [ProximityPredicate.NNS] and [ProximityPredicate.FNS]
     */
    sealed class KLimited<T : ProximityPredicate.KLimitedSearch>(tx: VAFIndex.Tx, context: BindingContext, partition: LongRange, predicate: T) : VAFCursor<T>(tx, context, partition, predicate) {

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
         * Prepares the [HeapSelection] for this [VAFCursor.KLimited]
         *
         * @return [HeapSelection]
         */
        protected abstract fun prepareVASSA(): HeapSelection<Tuple>
    }

    /**
     * A [VAFCursor] implementation for nearest neighbour search.
     */
    class NNS(tx: VAFIndex.Tx, context: BindingContext, partition: LongRange, predicate: ProximityPredicate.NNS) : KLimited<ProximityPredicate.NNS>(tx, context, partition, predicate) {
        /**
         * Prepares the result set using the [VAFIndex] and the VA-SSA algorithm described in [1].
         *
         * @return Prepared [HeapSelection]
         */
        override fun prepareVASSA(): HeapSelection<Tuple> {
            val localSelection = HeapSelection(this.predicate.k.toInt(), RecordComparator.SingleNonNullColumnComparator(this.predicate.distanceColumn, SortOrder.ASCENDING))
            try {
                /* First phase: Just add entries until we have k-results. */
                var threshold: Double
                do {
                    val tupleId = LongBinding.compressedEntryToLong(cursor.key)
                    val value = this.columnTx.read(tupleId)
                    val distance = this.predicate.distance(this.query, value)!!
                    localSelection.offer(StandaloneTuple(tupleId, this.produces, arrayOf(distance, value)))
                } while (localSelection.size < localSelection.k && this.cursor.next && this.cursor.key <= this.endKey)

                /* Second phase: Use lower-bound to decide whether entry should be added. */
                threshold = (localSelection.peek()!![0] as DoubleValue).value
                while (this.cursor.next && this.cursor.key <= this.endKey) {
                    val signature = VAFSignature.fromEntry(cursor.value)
                    if (this.bounds.lb(signature, threshold) < threshold) {
                        val tupleId = LongBinding.compressedEntryToLong(cursor.key)
                        val value = this.columnTx.read(tupleId)
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
     * A [VAFCursor] implementation for farthest neighbour search.
     */
    class FNS(tx: VAFIndex.Tx, context: BindingContext, partition: LongRange, predicate: ProximityPredicate.FNS) : KLimited<ProximityPredicate.FNS>(tx, context, partition, predicate) {
        /**
         * Prepares the result set using the [VAFIndex] and the VA-SSA algorithm described in [1].
         *
         * @return Prepared [HeapSelection]
         */
        override fun prepareVASSA(): HeapSelection<Tuple> {
            val localSelection = HeapSelection(this.predicate.k.toInt(), RecordComparator.SingleNonNullColumnComparator(this.predicate.distanceColumn, SortOrder.DESCENDING))
            try {
                /* First phase: Just add entries until we have k-results. */
                var threshold: Double
                do {
                    val tupleId = LongBinding.compressedEntryToLong(cursor.key)
                    val value = this.columnTx.read(tupleId)
                    val distance = this.predicate.distance(this.query, value)!!
                    localSelection.offer(StandaloneTuple(tupleId, this.produces, arrayOf(distance, value)))
                } while (localSelection.size < localSelection.k && this.cursor.next && this.cursor.key <= this.endKey)

                /* Second phase: Use lower-bound to decide whether entry should be added. */
                threshold = (localSelection.peek()!![0] as DoubleValue).value
                while (this.cursor.next && this.cursor.key <= this.endKey) {
                    val signature = VAFSignature.fromEntry(cursor.value)
                    if (this.bounds.ub(signature, threshold) < threshold) {
                        val tupleId = LongBinding.compressedEntryToLong(cursor.key)
                        val value = this.columnTx.read(tupleId)
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
     * An (experimental) [VAFCursor] implementation for range search.
     */
    class ENN(tx: VAFIndex.Tx, context: BindingContext, partition: LongRange, predicate: ProximityPredicate.ENN) : VAFCursor<ProximityPredicate.ENN>(tx, context, partition, predicate) {

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
            val value = this.columnTx.read(tupleId)
            val distance = this.predicate.distance(this.query, value)!!
            return StandaloneTuple(tupleId, this.produces, arrayOf(distance, value))
        }
    }
}