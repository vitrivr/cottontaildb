package org.vitrivr.cottontail.dbms.column

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.TabletId
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.tablets.Tablet
import org.vitrivr.cottontail.dbms.column.ColumnMetadata.Companion.storeName
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
/**
 * A [Cursor] to iterate over the [Tablet]s of a [Column].
 */
class FixedLengthCursor<T: Value>(column: FixedLengthColumn<T>.Tx): Cursor<T?> {

    /** The Xodus transaction snapshot used by this FixedLengthCursor. */
    private val xodusTx = column.xodusTx.readonlySnapshot

    /** */
    private val file = column.file

    /** Internal data [Store] reference. */
    private val store: Store = this.xodusTx.environment.openStore(
        column.dbo.name.storeName(),
        StoreConfig.USE_EXISTING,
        this.xodusTx,
        false
    ) ?: throw DatabaseException.DataCorruptionException("Data store for column ${column.dbo.name} is missing.")

    /** Internal Xodus cursor instance.  */
    private val cursor = this.store.openCursor(this.xodusTx)

    /** The [TabletId] of the currently loaded [Tablet]. -1 if no [Tablet] has been loaded. */
    private var tupleId: TupleId = -1

    override fun moveNext(): Boolean = this.moveTo(this.tupleId + 1)
    override fun movePrevious(): Boolean  = this.moveTo(this.tupleId - 1)
    override fun moveTo(tupleId: TupleId): Boolean {
        if (tupleId < 0) return false
        return when (tupleId) {
            this.tupleId -> true
            this.tupleId + 1 -> {
                if (this.cursor.next) {
                    this.tupleId = tupleId
                    true
                } else {
                    false
                }
            }
            this.tupleId - 1 -> {
                if (this.cursor.prev) {
                    this.tupleId = tupleId
                    true
                } else {
                    false
                }
            }
            else -> this.cursor.getSearchKey(LongBinding.longToCompressedEntry(tupleId)) != null
        }
    }

    override fun key(): TupleId = this.tupleId
    override fun value(): T = this.file.get(LongBinding.entryToLong(this.cursor.key))
    override fun close() {
        this.cursor.close()
        this.xodusTx.abort()
    }
}