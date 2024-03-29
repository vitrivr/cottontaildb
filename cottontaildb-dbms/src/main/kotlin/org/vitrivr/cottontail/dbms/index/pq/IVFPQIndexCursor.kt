package org.vitrivr.cottontail.dbms.index.pq

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.ShortBinding
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.binding.MissingTuple
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.RealVectorValue
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.dbms.index.pq.quantizer.PQCodebook
import org.vitrivr.cottontail.dbms.index.pq.signature.IVFPQSignature
import org.vitrivr.cottontail.dbms.index.pq.signature.PQLookupTable
import org.vitrivr.cottontail.utilities.selection.ComparablePair
import org.vitrivr.cottontail.utilities.selection.MinHeapSelection
import java.util.*
import java.util.concurrent.atomic.AtomicBoolean

/**
 * A [Cursor] implementation for the [IVFPQIndexCursor].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class IVFPQIndexCursor(val predicate: ProximityPredicate.Scan, val index: IVFPQIndex.Tx): Cursor<Tuple> {

    /** The [PQCodebook] used for coarse quantization. */
    private val coarse: PQCodebook = this.index.quantizer.coarse

    /** [PQLookupTable]s for the given query vector(s). */
    private val lookupTable: PQLookupTable

    /** The sub-transaction this [Cursor] operates upon.  */
    private val subTx = this.index.context.txn.xodusTx.readonlySnapshot

    /** The internal cursor used by this index. */
    private val cursor = this.index.dataStore.openCursor(this.subTx)

    /** The [ColumnDef] produced by  this [Cursor]. */
    private val produces = this.index.columnsFor(predicate).toTypedArray()

    /** A [Deque] for bucket numbers left to explore. */
    private val queue: Deque<Short>

    /** A begin of cursor flag. */
    private val boc = AtomicBoolean(false)

    init {
        /* Prepare list of Voronoi cells that should be scanned. */
        val nprobe = this@IVFPQIndexCursor.coarse.numberOfCentroids / 32
        val selection = MinHeapSelection<ComparablePair<Int,Double>>(nprobe)

        with(MissingTuple) {
            with(this@IVFPQIndexCursor.index.context.bindings) {
                this@IVFPQIndexCursor.lookupTable = this@IVFPQIndexCursor.index.quantizer.createLookupTable(this@IVFPQIndexCursor.predicate.query.getValue() as VectorValue<*>)
                for (c in coarse.centroids.indices) {
                    selection.offer(ComparablePair(c, this@IVFPQIndexCursor.coarse.distanceFrom(this@IVFPQIndexCursor.predicate.query.getValue() as RealVectorValue<*>, c)))
                }
            }
        }

        this.queue = ArrayDeque(nprobe)
        for (i in 0 until selection.size) {
            this.queue.offer(selection[i].first.toShort())
        }

        /* Move cursor to first entry. */
        if (this.nextCell()) {
            this.boc.compareAndExchange(false, true)
        }
    }

    /**
     * Moves the internal cursor and return true, as long as new candidates appear.
     */
    override fun moveNext(): Boolean
        = this.boc.compareAndExchange(true, false) || this.cursor.nextDup || this.nextCell()

    /**
     * Returns the current [TupleId] this [Cursor] is pointing to.
     *
     * @return [TupleId]
     */
    override fun key(): TupleId = LongBinding.compressedEntryToLong(this.cursor.value)

    /**
     * Returns the current [Tuple] this [Cursor] is pointing to.
     *
     * @return [TupleId]
     */
    override fun value(): Tuple {
        val signature = IVFPQSignature.fromEntry(this.cursor.value)
        val approximation = DoubleValue(this.lookupTable.approximateDistance(signature))
        return StandaloneTuple(signature.tupleId, this.produces, arrayOf(approximation))
    }

    /**
     * Closes this [IVFPQIndexCursor]
     */
    override fun close() {
        this.subTx.abort()
        this.cursor.close()
    }

    /**
     * Moves to the next cell in the list of cells that should be inspected.
     * If no more cell is available, this method returns false.
     *
     * @return True if next cell was dequeued
     */
    private fun nextCell(): Boolean {
        val next = this.queue.poll() ?: return false
        return this.cursor.getSearchKey(ShortBinding.shortToEntry(next)) != null
    }
}