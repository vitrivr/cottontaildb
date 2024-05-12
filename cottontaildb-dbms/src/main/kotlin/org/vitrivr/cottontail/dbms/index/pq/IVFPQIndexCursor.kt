package org.vitrivr.cottontail.dbms.index.pq

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.ShortBinding
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.dbms.index.basic.IndexMetadata.Companion.storeName
import org.vitrivr.cottontail.dbms.index.pq.quantizer.MultiStageQuantizer
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
class IVFPQIndexCursor(query: VectorValue<*>, quantizer: MultiStageQuantizer, private val columns: Array<ColumnDef<*>>, index: IVFPQIndex.Tx): Cursor<Tuple> {

    /** [PQLookupTable]s for the given query vector(s). */
    private val lookupTable: PQLookupTable = quantizer.createLookupTable(query)

    /** The sub-transaction this [Cursor] operates upon.  */
    private val xodusTx = index.xodusTx.readonlySnapshot

    /** The internal cursor used by this index. */
    private val store = this.xodusTx.environment.openStore(index.dbo.name.storeName(), StoreConfig.USE_EXISTING,  this.xodusTx)

    /** Cursor backing this [PQIndexCursor]. */
    private val cursor: jetbrains.exodus.env.Cursor = this.store.openCursor(this.xodusTx)

    /** The sub-transaction this [Cursor] operates upon.  */

    /** A [Deque] for bucket numbers left to explore. */
    private val queue: Deque<Short>

    /** A beginning of cursor flag. */
    private val boc = AtomicBoolean(false)

    init {
        /* Prepare list of Voronoi cells that should be scanned. */
        val nprobe = quantizer.coarse.numberOfCentroids / 32
        val selection = MinHeapSelection<ComparablePair<Int,Double>>(nprobe)

        for (c in quantizer.coarse.centroids.indices) {
            selection.offer(ComparablePair(c, quantizer.coarse.distanceFrom(query, c)))
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
        return StandaloneTuple(signature.tupleId, this.columns, arrayOf(approximation))
    }

    /**
     * Closes this [IVFPQIndexCursor]
     */
    override fun close() {
        this.xodusTx.abort()
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