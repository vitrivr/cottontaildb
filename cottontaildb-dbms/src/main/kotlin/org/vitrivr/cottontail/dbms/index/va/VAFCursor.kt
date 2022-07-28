package org.vitrivr.cottontail.dbms.index.va

import jetbrains.exodus.bindings.LongBinding
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.binding.Binding
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

/**
 * A [Cursor] implementation for the [VAFIndex].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@Suppress("UNCHECKED_CAST")
class VAFCursor(val partition: LongRange, val predicate: ProximityPredicate, val index: VAFIndex.Tx): Cursor<Record> {
    /** [VectorValue] used for query. Must be prepared before using the [Iterator]. */
    private val query: RealVectorValue<*>

    /** The [Bounds] objects used for filtering. */
    private val bounds: Bounds

    /** Internal [ColumnTx] used to access actual values. */
    private val columnCursor: Cursor<RealVectorValue<*>?>

    /** The [TupleId] to start with. */
    private val startKey = partition.first.toKey()

    /** The [TupleId] to end at. */
    private val endKey = partition.last.toKey()

    /** The [HeapSelection] use for finding the top k entries. */
    private val selection = when (this.predicate) {
        is ProximityPredicate.NNS -> HeapSelection(this.predicate.k, RecordComparator.SingleNonNullColumnComparator(this.predicate.distanceColumn, SortOrder.ASCENDING))
        is ProximityPredicate.FNS -> HeapSelection(this.predicate.k, RecordComparator.SingleNonNullColumnComparator(this.predicate.distanceColumn, SortOrder.DESCENDING))
        else -> throw IllegalArgumentException("VAFIndex does only support NNS and FNS queries. This is a programmer's error!")
    }

    /** Cached in-memory version of the [EquidistantVAFMarks] used by this [Cursor]. */
    private val marks = this.index.marks

    /** The columns produced by this [Cursor]. */
    private val produces = this.index.columnsFor(this.predicate).toTypedArray()

    /** The current [Cursor] position. */
    private var position = -1L

    init {
        /* Convert query vector. */
        val queryVectorBinding = this@VAFCursor.predicate.query
        require(queryVectorBinding is Binding.Literal) {  "Bound query vector is not a literal value."  }
        with(MissingRecord) {
            with(this@VAFCursor.index.context.bindings) {
                val value = queryVectorBinding.getValue()
                check(value is RealVectorValue<*>) { "Bound value for query vector has wrong type (found = ${value?.type})." }
                this@VAFCursor.query = value
            }
        }

        /* Obtain Tx object for column. */
        val entityTx: EntityTx = this.index.dbo.parent.newTx(this.index.context)
        this.columnCursor = entityTx.columnForName(this.index.columns[0].name).newTx(this.index.context).cursor(partition) as Cursor<RealVectorValue<*>?>

        /* Derive bounds object. */
        this.bounds = when (this.predicate.distance) {
            is ManhattanDistance<*> -> L1Bounds(this.query, this.marks)
            is EuclideanDistance<*>,
            is SquaredEuclideanDistance<*> -> L2Bounds(this.query, this.marks)
            else -> throw IllegalArgumentException("The ${this.predicate.distance} distance kernel is not supported by VAFIndex.")
        }
    }

    /**
     * Moves the internal cursor and return true, as long as new candidates appear.
     */
    override fun moveNext(): Boolean {
        if (this.selection.added == 0L) this.prepareVASSA()
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
     * Closes this [Cursor]
     */
    override fun close() {
        this.columnCursor.close()
    }

    /**
     * Reads the vector with the given [TupleId] and adds it to the [HeapSelection].
     *
     * @param tupleId The [TupleId] to read.
     */
    private fun readAndOffer(tupleId: TupleId): Double {
        require(this.columnCursor.moveTo(tupleId)) { "Column cursor failed to seek tuple with ID ${tupleId}."}
        val value = this.columnCursor.value()
        val distance = this.predicate.distance(this.query, value)!!
        return (this.selection.offer(StandaloneRecord(tupleId, this.produces, arrayOf(distance, value)))[0] as DoubleValue).value
    }

    /**
     * Prepares the result set using the [VAFIndex] and the VA-SSA algorithm described in [1].
     */
    private fun prepareVASSA() {
        /* Initialize cursor. */
        val subTx = this.index.context.txn.xodusTx.readonlySnapshot
        val cursor = this.index.dataStore.openCursor(subTx)
        try {
            if (cursor.getSearchKeyRange(this.startKey) == null) return

            /* First phase: Just add entries until we have k-results. */
            var threshold: Double
            do {
                val tupleId = LongBinding.compressedEntryToLong(cursor.key)
                threshold = this.readAndOffer(tupleId)
            } while (this.selection.added < this.selection.k && cursor.next && cursor.key < this.endKey)

            /* Second phase: Use lower-bound to decide whether entry should be added. */
            do {
                val signature = VAFSignature.fromEntry(cursor.value)
                if (this.bounds.lb(signature, threshold) < threshold) {
                    val tupleId = LongBinding.compressedEntryToLong(cursor.key)
                    threshold = this.readAndOffer(tupleId)
                }
            } while (cursor.next && cursor.key < this.endKey)
        } catch (e: Throwable) {
            VAFIndex.LOGGER.error("VA-SSA Scan: Error while scanning VAF index: ${e.message}")
        } finally {
            /* Log efficiency of VAF scan. */
            VAFIndex.LOGGER.debug("VA-SSA Scan: Read ${this.selection.added} and skipped over ${(1.0 - (this.selection.added.toDouble() / this.index.count())) * 100}% of entries.")

            /* Close Xodus cursor. */
            cursor.close()
            subTx.abort()
        }
    }
}