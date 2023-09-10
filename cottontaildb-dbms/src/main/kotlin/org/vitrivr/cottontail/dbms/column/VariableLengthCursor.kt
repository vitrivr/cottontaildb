package org.vitrivr.cottontail.dbms.column

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.Transaction
import org.vitrivr.cottontail.storage.serializers.SerializerFactory
import org.vitrivr.cottontail.storage.serializers.values.ValueSerializer

/**
 *
 */
class VariableLengthCursor<T: Value>(private val column: Column<T>, private val transaction: Transaction): Cursor<T> {
    /** Internal data [Store] reference. */
    private val dataStore: Store = this.column.catalogue.transactionManager.environment.openStore(
        this.column.name.storeName(),
        StoreConfig.USE_EXISTING,
        this.transaction.xodusTx,
        false
    ) ?: throw DatabaseException.DataCorruptionException("Data store for column ${this.column.name} is missing.")

    /** The internal [ValueSerializer] reference used for de-/serialization. */
    private val binding: ValueSerializer<T> = SerializerFactory.value(this.column.columnDef.type)

    /** Internal Xodus cursor instance.  */
    private val cursor = this.dataStore.openCursor(this.transaction.xodusTx)
    override fun moveNext(): Boolean = this.cursor.next
    override fun key(): TupleId = LongBinding.compressedEntryToLong(this.cursor.key)
    override fun value(): T = this.binding.fromEntry(this.cursor.value)!!
    override fun close() = this.cursor.close()
}