package org.vitrivr.cottontail.dbms.index.pq

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.queries.binding.BindingContext
import org.vitrivr.cottontail.core.queries.binding.MissingTuple
import org.vitrivr.cottontail.core.queries.predicates.ProximityPredicate
import org.vitrivr.cottontail.core.tuple.StandaloneTuple
import org.vitrivr.cottontail.core.tuple.Tuple
import org.vitrivr.cottontail.core.values.DoubleValue
import org.vitrivr.cottontail.core.values.VectorValue
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.index.pq.signature.PQLookupTable
import org.vitrivr.cottontail.dbms.index.pq.signature.SPQSignature
import org.vitrivr.cottontail.dbms.index.va.VAFCursor
import org.vitrivr.cottontail.dbms.index.va.VAFIndex
import java.util.concurrent.atomic.AtomicBoolean

/**
 * A [Cursor] implementation for the [PQIndex].
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class PQIndexCursor(index: PQIndex.Tx, context: BindingContext, partition: LongRange, val predicate: ProximityPredicate.Scan): Cursor<Tuple> {
    /** Prepares [PQLookupTable]s for the given query vector(s). */
    private val lookupTable: PQLookupTable

    /** The sub-transaction used by this [PQIndexCursor]. */
    private val xodusTx: Transaction = index.xodusTx.readonlySnapshot

    /** The store containing the [VAFIndex] entries. */
    private val store: Store = this.xodusTx.environment.openStore(index.dbo.name.storeName(), StoreConfig.USE_EXISTING, this.xodusTx)

    /** Cursor backing this [VAFCursor]. */
    private val cursor = this.store.openCursor(this.xodusTx)

    /** The start key. */
    private val startKey = partition.first.toKey()

    /* The end key. */
    private val endKey = partition.last.toKey()

    /** The [ColumnDef] produced by  this [Cursor]. */
    private val produces = index.columnsFor(predicate).toTypedArray()

    /** A begin of cursor flag. */
    private val boc = AtomicBoolean(true)

    init {
        /* Obtain query vector. Requires a binding context! */
        this.lookupTable = with(context) {
            with(MissingTuple) {
                val query = this@PQIndexCursor.predicate.query.getValue() as? VectorValue<*> ?: throw IllegalArgumentException("The query vector for a PQIndex must be a VectorValue. This is a progarmmer's error!")
                index.quantizer.createLookupTable(query)
            }
        }

        /* Initialize cursor. */
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
        return StandaloneTuple(LongBinding.compressedEntryToLong(cursor.key), this.produces, arrayOf(approximation))
    }

    /**
     * Closes this [PQIndexCursor].
     */
    override fun close() {
        this.cursor.close()
        this.xodusTx.abort()
    }
}