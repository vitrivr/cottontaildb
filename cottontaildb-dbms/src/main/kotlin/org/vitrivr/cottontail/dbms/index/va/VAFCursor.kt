package org.vitrivr.cottontail.dbms.index.va

import jetbrains.exodus.bindings.LongBinding
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.binding.MissingRecord
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.EuclideanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.ManhattanDistance
import org.vitrivr.cottontail.core.queries.functions.math.distance.binary.SquaredEuclideanDistance
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.core.queries.sort.SortOrder
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.types.RealVectorValue
import org.vitrivr.cottontail.core.values.types.VectorValue
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.column.ColumnTx
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.execution.operators.sort.RecordComparator
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
 * @version 1.0.0
 */
@Suppress("UNCHECKED_CAST")
sealed class VAFCursor<T: ProximityPredicate>(protected val partition: LongRange, protected val predicate: T, protected val index: VAFIndex.Tx): Cursor<Record> {
    /** [VectorValue] used for query. Must be prepared before using the [Iterator]. */
    protected val query: RealVectorValue<*>

    /** The [Bounds] objects used for filtering. */
    protected val bounds: Bounds

    /** Internal [ColumnTx] used to access actual values. */
    protected val columnCursor: Cursor<RealVectorValue<*>?>

    /** The [TupleId] to start with. */
    protected val startKey = this.partition.first.toKey()

    /** The [TupleId] to end at. */
    protected val endKey = this.partition.last.toKey()

    /** Cached in-memory version of the [EquidistantVAFMarks] used by this [Cursor]. */
    protected val marks = this.index.marks

    /** The columns produced by this [Cursor]. */
    protected val produces = this.index.columnsFor(this.predicate).toTypedArray()

    init {
        /* Extract query vector from binding. */
        val queryVectorBinding = this.predicate.query
        with(MissingRecord) {
            with(this@VAFCursor.index.context.bindings) {
                val value = queryVectorBinding.getValue()
                check(value is RealVectorValue<*>) { "Bound value for query vector has wrong type (found = ${value?.type})." }
                this@VAFCursor.query = value
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
        val entityTx: EntityTx = this.index.dbo.parent.newTx(this.index.context)
        this.columnCursor = entityTx.columnForName(this.index.columns[0].name).newTx(this.index.context).cursor(this.partition) as Cursor<RealVectorValue<*>?>
    }

    /**
     * Closes this [Cursor]
     */
    override fun close() {
        this.columnCursor.close()
    }

    /**
     * A [VAFCursor] for limiting [ProximityPredicate], i.e., [ProximityPredicate.NNS] and [ProximityPredicate.FNS]
     */
    sealed class KLimited<T : ProximityPredicate.KLimitedSearch>(partition: LongRange, predicate: T, index: VAFIndex.Tx) : VAFCursor<T>(partition, predicate, index) {

        /** The [HeapSelection] use for finding the top k entries. */
        protected var selection: HeapSelection<Record>? = null

        /** */
        protected var current: Record? = null

        /**
         * Moves the internal cursor and return true, as long as new candidates appear.
         */
        override fun moveNext(): Boolean {
            if (this.selection == null) { /* Initialize data. */
                this.selection = this.prepareVASSA()
            }
            if (this.selection!!.isEmpty()) return false
            this.current = this.selection!!.dequeue()
            return true
        }

        /**
         * Returns the current [TupleId] this [Cursor] is pointing to.
         *
         * @return [TupleId]
         */
        override fun key(): TupleId = this.current?.tupleId ?: throw IllegalStateException("VAFCursor has been depleted.")

        /**
         * Returns the current [Record] this [Cursor] is pointing to.
         *
         * @return [TupleId]
         */
        override fun value(): Record = this.current ?: throw IllegalStateException("VAFCursor has been depleted.")

        /**
         * Reads the vector with the given [TupleId] and adds it to the [HeapSelection].
         *
         * @param tupleId The [TupleId] to read.
         */
        protected fun readAndOffer(tupleId: TupleId): Double {
            require(this.columnCursor.moveTo(tupleId)) { "Column cursor failed to seek tuple with ID ${tupleId}." }
            val value = this.columnCursor.value()
            val distance = this.predicate.distance(this.query, value)!!
            return (this.selection!!.enqueue(StandaloneRecord(tupleId, this.produces, arrayOf(distance, value)))[0] as DoubleValue).value
        }

        protected abstract fun prepareVASSA(): HeapSelection<Record>
    }

    /**
     * A [VAFCursor] implementation for nearest neighbour search.
     */
    class NNS(partition: LongRange, predicate: ProximityPredicate.NNS, index: VAFIndex.Tx) : KLimited<ProximityPredicate.NNS>(partition, predicate, index) {
        /**
         * Prepares the result set using the [VAFIndex] and the VA-SSA algorithm described in [1].
         *
         * @return Prepared [HeapSelection]
         */
        override fun prepareVASSA(): HeapSelection<Record> {
            /* Initialize cursor. */
            val subTx = this.index.context.txn.xodusTx.readonlySnapshot
            val cursor = this.index.dataStore.openCursor(subTx)
            try {
                if (cursor.getSearchKeyRange(this.startKey) == null)
                    return HeapSelection(0, RecordComparator.SingleNonNullColumnComparator(this.predicate.distanceColumn, SortOrder.ASCENDING))

                /* First phase: Just add entries until we have k-results. */
                val heap = arrayOfNulls<Record?>(this.predicate.k.toInt())
                var index = 0
                do {
                    val tupleId = LongBinding.compressedEntryToLong(cursor.key)
                    require(this.columnCursor.moveTo(tupleId)) { "Column cursor failed to seek tuple with ID ${tupleId}." }
                    val value = this.columnCursor.value()
                    val distance = this.predicate.distance(this.query, value)!!
                    heap[index++] = StandaloneRecord(tupleId, this.produces, arrayOf(distance, value))
                } while (index < this.predicate.k && cursor.next && cursor.key < this.endKey)

                /* Early abort, if partition contains less than k entries. */
                if (index < this.predicate.k - 1) {
                    return HeapSelection(heap.copyOfRange(0, index), RecordComparator.SingleNonNullColumnComparator(this.predicate.distanceColumn, SortOrder.ASCENDING))
                }

                /* Second phase: Use lower-bound to decide whether entry should be added. */
                val localSelection = HeapSelection(heap, RecordComparator.SingleNonNullColumnComparator(this.predicate.distanceColumn, SortOrder.ASCENDING))
                var threshold: Double = Double.MAX_VALUE
                do {
                    val signature = VAFSignature.fromEntry(cursor.value)
                    if (this.bounds.lb(signature, threshold) < threshold) {
                        val tupleId = LongBinding.compressedEntryToLong(cursor.key)
                        require(this.columnCursor.moveTo(tupleId)) { "Column cursor failed to seek tuple with ID ${tupleId}." }
                        val value = this.columnCursor.value()
                        val distance = this.predicate.distance(this.query, value)!!
                        threshold = (localSelection.enqueue(StandaloneRecord(tupleId, this.produces, arrayOf(distance, value)))[0] as DoubleValue).value
                    }
                } while (cursor.next && cursor.key < this.endKey)

                /* Log efficiency of VAF scan. */
                VAFIndex.LOGGER.debug("VA-SSA Scan: Read ${localSelection.added} and skipped over ${(1.0 - (localSelection.added.toDouble() / this.index.count())) * 100}% of entries.")
                return localSelection
            } catch (e: Throwable) {
                VAFIndex.LOGGER.error("VA-SSA Scan: Error while scanning VAF index: ${e.message}")
                return HeapSelection(0, RecordComparator.SingleNonNullColumnComparator(this.predicate.distanceColumn, SortOrder.ASCENDING))
            } finally {
                /* Close Xodus cursor. */
                cursor.close()
                subTx.abort()
            }
        }
    }

    /**
     * A [VAFCursor] implementation for farthest neighbour search.
     */
    class FNS(partition: LongRange, predicate: ProximityPredicate.FNS, index: VAFIndex.Tx) : KLimited<ProximityPredicate.FNS>(partition, predicate, index) {
        /**
         * Prepares the result set using the [VAFIndex] and the VA-SSA algorithm described in [1].
         *
         * @return Prepared [HeapSelection]
         */
        override fun prepareVASSA(): HeapSelection<Record> {
            /* Initialize cursor. */
            val subTx = this.index.context.txn.xodusTx.readonlySnapshot
            val cursor = this.index.dataStore.openCursor(subTx)
            try {
                if (cursor.getSearchKeyRange(this.startKey) == null)
                    return HeapSelection(0, RecordComparator.SingleNonNullColumnComparator(this.predicate.distanceColumn, SortOrder.DESCENDING))

                /* First phase: Just add entries until we have k-results. */
                val heap = arrayOfNulls<Record?>(this.predicate.k.toInt())
                var index = 0
                do {
                    val tupleId = LongBinding.compressedEntryToLong(cursor.key)
                    require(this.columnCursor.moveTo(tupleId)) { "Column cursor failed to seek tuple with ID ${tupleId}." }
                    val value = this.columnCursor.value()
                    val distance = this.predicate.distance(this.query, value)!!
                    heap[index++] = StandaloneRecord(tupleId, this.produces, arrayOf(distance, value))
                } while (index < this.predicate.k && cursor.next && cursor.key < this.endKey)

                /* Early abort, if partition contains less than k entries. */
                if (index < this.predicate.k - 1) {
                    return HeapSelection(heap.copyOfRange(0, index), RecordComparator.SingleNonNullColumnComparator(this.predicate.distanceColumn, SortOrder.DESCENDING))
                }

                /* Second phase: Use lower-bound to decide whether entry should be added. */
                val localSelection = HeapSelection(heap, RecordComparator.SingleNonNullColumnComparator(this.predicate.distanceColumn, SortOrder.DESCENDING))
                var threshold: Double = Double.MAX_VALUE
                do {
                    val signature = VAFSignature.fromEntry(cursor.value)
                    if (this.bounds.ub(signature, threshold) > threshold) {
                        val tupleId = LongBinding.compressedEntryToLong(cursor.key)
                        require(this.columnCursor.moveTo(tupleId)) { "Column cursor failed to seek tuple with ID ${tupleId}." }
                        val value = this.columnCursor.value()
                        val distance = this.predicate.distance(this.query, value)!!
                        threshold = (localSelection.enqueue(StandaloneRecord(tupleId, this.produces, arrayOf(distance, value)))[0] as DoubleValue).value
                    }
                } while (cursor.next && cursor.key < this.endKey)

                /* Log efficiency of VAF scan. */
                VAFIndex.LOGGER.debug("VA-SSA Scan: Read ${localSelection.added} and skipped over ${(1.0 - (localSelection.added.toDouble() / this.index.count())) * 100}% of entries.")
                return localSelection
            } catch (e: Throwable) {
                VAFIndex.LOGGER.error("VA-SSA Scan: Error while scanning VAF index: ${e.message}")
                return HeapSelection(0, RecordComparator.SingleNonNullColumnComparator(this.predicate.distanceColumn, SortOrder.DESCENDING))
            } finally {
                /* Close Xodus cursor. */
                cursor.close()
                subTx.abort()
            }
        }
    }

    /**
     * An (experimental) [VAFCursor] implementation for range search.
     */
    class ENN(partition: LongRange, predicate: ProximityPredicate.ENN, index: VAFIndex.Tx) : VAFCursor<ProximityPredicate.ENN>(partition, predicate, index) {

        /* Sub-transaction this cursor uses. */
        private val subTx = this.index.context.txn.xodusTx.readonlySnapshot

        /** Actual index cursor instance. */
        private  val cursor = this.index.dataStore.openCursor(subTx)

        /** A begin of cursor (BoC) flag. */
        protected val boc = AtomicBoolean(false)

        init {
            if (this.cursor.getSearchKeyRange(this.startKey) != null) {
                this.boc.compareAndExchange(false, true)
            }
        }

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

        override fun key(): TupleId = this.columnCursor.key()
        override fun value(): Record {
            val tupleId = this.columnCursor.key()
            val value = this.columnCursor.value()
            val distance = this.predicate.distance(this.query, value)!!
            return StandaloneRecord(tupleId, this.produces, arrayOf(distance, value))
        }

        /**
         * Closes this [VAFCursor.ENN].
         */
        override fun close() {
            super.close()
            this.cursor.close()
            this.subTx.abort()
        }
    }
}