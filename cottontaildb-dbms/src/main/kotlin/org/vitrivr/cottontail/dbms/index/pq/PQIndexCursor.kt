package org.vitrivr.cottontail.dbms.index.pq

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.types.VectorValue
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.entity.values.StoredTuple
import org.vitrivr.cottontail.dbms.entity.values.StoredValue
import org.vitrivr.cottontail.dbms.index.basic.IndexMetadata.Companion.storeName
import org.vitrivr.cottontail.dbms.index.pq.quantizer.SingleStageQuantizer
import org.vitrivr.cottontail.dbms.index.pq.signature.PQLookupTable
import org.vitrivr.cottontail.dbms.index.pq.signature.SPQSignature
import java.util.concurrent.atomic.AtomicBoolean

/**
 * A [Cursor] implementation for the [PQIndex].
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
class PQIndexCursor(partition: LongRange, query: VectorValue<*>, quantizer: SingleStageQuantizer, private val columns: Array<ColumnDef<*>>, private val index: PQIndex.Tx): Cursor<Tuple> {
    /** Prepares [PQLookupTable]s for the given query vector(s). */
    private val lookupTable: PQLookupTable = quantizer.createLookupTable(query)

    /** The sub-transaction this [Cursor] operates upon.  */
    private val xodusTx = index.xodusTx.readonlySnapshot

    /** The internal cursor used by this index. */
    private val store = this.xodusTx.environment.openStore(index.dbo.name.storeName(), StoreConfig.USE_EXISTING,  this.xodusTx)

    /** Cursor backing this [PQIndexCursor]. */
    private val cursor: jetbrains.exodus.env.Cursor = this.store.openCursor(this.xodusTx)

    /** The start key. */
    private val startKey = partition.first.toKey()

    /* The end key. */
    private val endKey = partition.last.toKey()

    /** A beginning of cursor flag. */
    private val boc = AtomicBoolean(true)

    init {
        if (this.cursor.getSearchKeyRange(this.startKey) == null) {
            this.boc.set(false)
        }
    }

    /**
     * Moves the internal cursor and return true, as long as new candidates appear.
     */
    override fun moveNext(): Boolean
        = (this.boc.compareAndExchange(true, false) || (this.cursor.next && this.cursor.key <= this.endKey))

    /**
     * Returns the current [TupleId] this [Cursor] is pointing to.
     *
     * @return [TupleId]
     */
    override fun key(): TupleId = LongBinding.compressedEntryToLong(this.cursor.key)

    /**
     * Returns the current [Tuple] this [Cursor] is pointing to.
     *
     * @return [TupleId]
     */
    override fun value(): Tuple {
        val signature = SPQSignature.fromEntry(this.cursor.value)
        val approximation = DoubleValue(this.lookupTable.approximateDistance(signature))
        val tupleId = LongBinding.compressedEntryToLong(cursor.key)
        val tuple = this@PQIndexCursor.index.parent.read(tupleId) as StoredTuple
        return StoredTuple(tupleId, this.columns, arrayOf(*tuple.values, StoredValue.Inline(approximation)))
    }

    /**
     * Closes this [PQIndexCursor].
     */
    override fun close() {
        this.xodusTx.abort()
        this.cursor.close()
    }
}