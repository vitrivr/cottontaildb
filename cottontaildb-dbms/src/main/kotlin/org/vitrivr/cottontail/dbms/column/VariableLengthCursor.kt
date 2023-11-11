package org.vitrivr.cottontail.dbms.column

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.values.Value
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.storage.serializers.SerializerFactory
import org.vitrivr.cottontail.storage.serializers.values.ValueSerializer

/**
 *
 */
class VariableLengthCursor<T: Value>(tx: VariableLengthColumn<T>.Tx): Cursor<T> {
    /** The Xodus [jetbrains.exodus.env.Transaction]. */
    private val xodusTx: jetbrains.exodus.env.Transaction = tx.xodusTx.readonlySnapshot

    /** Internal data [Store] reference. */
    private val store: Store = this.xodusTx.environment.openStore(tx.dbo.name.storeName(), StoreConfig.USE_EXISTING, this.xodusTx, false)
        ?: throw DatabaseException.DataCorruptionException("Data store for column ${tx.dbo.name.storeName()} is missing.")

    /** The internal [ValueSerializer] reference used for de-/serialization. */
    private val binding: ValueSerializer<T> = SerializerFactory.value(tx.dbo.columnDef.type)

    /** Internal Xodus cursor instance.  */
    private val cursor = this.store.openCursor(this.xodusTx)
    override fun moveNext(): Boolean = this.cursor.next
    override fun moveTo(tupleId: TupleId): Boolean = this.cursor.getSearchKey(LongBinding.longToCompressedEntry(tupleId)) != null
    override fun key(): TupleId = LongBinding.compressedEntryToLong(this.cursor.key)
    override fun value(): T = this.binding.fromEntry(this.cursor.value)!!
    override fun close() {
        this.cursor.close()
        this.xodusTx.abort()
    }
}