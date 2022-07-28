package org.vitrivr.cottontail.dbms.index.pq

import jetbrains.exodus.bindings.LongBinding
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.core.recordset.PlaceholderRecord
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.types.VectorValue
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.index.pq.signature.PQLookupTable
import org.vitrivr.cottontail.dbms.index.pq.signature.SPQSignature
import java.util.concurrent.atomic.AtomicBoolean

/**
 * A [Cursor] implementation for the [PQIndex].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class PQIndexCursor(partition: LongRange, val predicate: ProximityPredicate.Scan, val index: PQIndex.Tx): Cursor<Record> {
        /** Prepares [PQLookupTable]s for the given query vector(s). */
    private val lookupTable: PQLookupTable

    /** The sub-transaction this [Cursor] operates upon.  */
    private val subTx = this.index.context.txn.xodusTx.readonlySnapshot

    /** The internal cursor used by this index. */
    private val cursor = this.index.dataStore.openCursor(this.subTx)

    /** The start key. */
    private val startKey = partition.first.toKey()

    /* The end key. */
    private val endKey = partition.last.toKey()

    /** The [ColumnDef] produced by  this [Cursor]. */
    private val produces = this.index.columnsFor(predicate).toTypedArray()

    /** A begin of cursor flag. */
    private val boc = AtomicBoolean(true)

    init {
        with(PlaceholderRecord) {
            with(this@PQIndexCursor.index.context.bindings) {
                this@PQIndexCursor.lookupTable = this@PQIndexCursor.index.quantizer.createLookupTable(this@PQIndexCursor.predicate.query.getValue() as VectorValue<*>)
            }
        }

        if (this.cursor.getSearchKeyRange(this.startKey) == null) {
            this.boc.set(false)
        }
    }

    /**
     * Moves the internal cursor and return true, as long as new candidates appear.
     */
    override fun moveNext(): Boolean
        = (this.boc.compareAndExchange(true, false) || (this.cursor.key < this.endKey && this.cursor.next))

    /**
     * Returns the current [TupleId] this [Cursor] is pointing to.
     *
     * @return [TupleId]
     */
    override fun key(): TupleId = LongBinding.compressedEntryToLong(this.cursor.key)

    /**
     * Returns the current [Record] this [Cursor] is pointing to.
     *
     * @return [TupleId]
     */
    override fun value(): Record {
        val signature = SPQSignature.fromEntry(this.cursor.value)
        val approximation = DoubleValue(this.lookupTable.approximateDistance(signature))
        return StandaloneRecord(LongBinding.compressedEntryToLong(cursor.key), this.produces, arrayOf(approximation))
    }

    /**
     * Closes this [PQIndexCursor].
     */
    override fun close() {
        this.subTx.abort()
        this.cursor.close()
    }
}