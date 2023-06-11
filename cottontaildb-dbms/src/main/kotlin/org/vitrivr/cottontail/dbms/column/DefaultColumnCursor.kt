package org.vitrivr.cottontail.dbms.column

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.bindings.LongBinding
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.storage.serializers.values.ValueSerializerFactory
import org.vitrivr.cottontail.storage.serializers.values.xodus.XodusBinding
import java.util.concurrent.atomic.AtomicBoolean

/**
 * A [Cursor] implementation for the [DefaultColumn].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class DefaultColumnCursor<T: Value>(private val partition: LongRange, private val tx: DefaultColumn<T>.Tx, private val context: QueryContext): Cursor<T?> {

    /** The per-[Cursor] [XodusBinding] instance. */
    private val binding: XodusBinding<T> = ValueSerializerFactory.xodus(this.tx.dbo.columnDef.type, this.tx.dbo.nullable)

    /** Creates a read-only snapshot of the enclosing Tx. */
    private val subTransaction = this.context.txn.xodusTx.readonlySnapshot

    /** Internal [Cursor] used for iteration. */
    private val cursor: jetbrains.exodus.env.Cursor = this.tx.dataStore.openCursor(this.subTransaction)

    /** The [TupleId] to start at. */
    private val startKey: ArrayByteIterable

    /** The [TupleId] to end at. */
    private val endKey: ArrayByteIterable

    /** Flag indicating, that data must be read from store. */
    private val boc = AtomicBoolean(true)

    init {
        if (this.partition.first < 0 || this.cursor.getSearchKeyRange(this.partition.first.toKey()) == null) {
            this.boc.set(false)
            this.startKey = LongBinding.longToEntry(-1L)
            this.endKey = LongBinding.longToEntry(-1L)
        } else {
            this.startKey = this.partition.first.toKey()
            this.endKey = this.partition.last.toKey()
        }
    }

    /**
     * Tries to move this [Cursor] to the next [TupleId].
     *
     * @return True on success, false otherwise,
     */
    override fun moveNext(): Boolean = (this.boc.compareAndExchange(true, false) || (this.cursor.next && this.cursor.key <= this.endKey))

    /**
     * Tries to move this [Cursor] to the previous [TupleId].
     *
     * @return True on success, false otherwise,
     */
    override fun movePrevious(): Boolean = (this.cursor.prev && this.cursor.key >= this.startKey)

    /**
     * Tries to move this [Cursor] to the provided [TupleId].
     *
     * @param tupleId The [TupleId] to move to.
     * @return True on success, false otherwise,
     */
    override fun moveTo(tupleId: TupleId): Boolean = this.partition.contains(tupleId) && this.cursor.getSearchKey(tupleId.toKey()) != null

    /**
     * Returns the [TupleId] this [DefaultColumnCursor] is currently pointing to.
     */
    override fun key(): TupleId = LongBinding.compressedEntryToLong(this.cursor.key)

    /**
     * Returns the [Value] this [DefaultColumnCursor] is currently pointing to.
     */
    override fun value(): T? = this.binding.entryToValue(this.cursor.value)

    /**
     * Closes this [DefaultColumnCursor] and invalidates the associated sub transaction.
     */
    override fun close() {
        this.cursor.close()
        this.subTransaction.abort()
    }
}