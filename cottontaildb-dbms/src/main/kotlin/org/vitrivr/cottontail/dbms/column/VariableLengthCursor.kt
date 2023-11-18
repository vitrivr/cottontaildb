package org.vitrivr.cottontail.dbms.column

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.storage.serializers.SerializerFactory
import org.vitrivr.cottontail.storage.serializers.values.ValueSerializer

/**
 *
 */
class VariableLengthCursor<T: Value>(column: VariableLengthColumn<T>.Tx): Cursor<T> {
    /** The Xodus transaction snapshot used by this FixedLengthCursor. */
    private val xodusTx = column.context.txn.xodusTx.readonlySnapshot

    /** Internal data [Store] reference. */
    private val store: Store = this.xodusTx.environment.openStore(
        column.dbo.name.storeName(),
        StoreConfig.USE_EXISTING,
        xodusTx,
        false
    ) ?: throw DatabaseException.DataCorruptionException("Data store for column ${column.dbo.name} is missing.")

    /** The internal [ValueSerializer] reference used for de-/serialization. */
    private val binding: ValueSerializer<T> = SerializerFactory.value(column.columnDef.type)

    /** Internal Xodus cursor instance.  */
    private val cursor = this.store.openCursor(this.xodusTx)

    /** The [TupleId] this [VariableLengthCursor] is currently pointing to. */
    private var tupleId: TupleId = -1L

    override fun moveNext(): Boolean = this.moveTo(this.tupleId + 1)
    override fun movePrevious(): Boolean  = this.moveTo(this.tupleId - 1)
    override fun moveTo(tupleId: TupleId): Boolean {
        val ret = when (tupleId) {
            this.tupleId + 1 -> this.cursor.next
            this.tupleId - 1 -> this.cursor.prev
            else -> this.cursor.getSearchKey(LongBinding.longToCompressedEntry(tupleId)) != null
        }
        if (ret) this.tupleId = tupleId
        return ret
    }

    override fun key(): TupleId = this.tupleId
    override fun value(): T = this.binding.fromEntry(this.cursor.value)!!
    override fun close() {
        this.cursor.close()
        this.xodusTx.abort()
    }
}