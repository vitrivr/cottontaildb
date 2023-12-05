package org.vitrivr.cottontail.dbms.column

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.TabletId
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.tablets.Tablet
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.column.FixedLengthColumn.Companion.TABLET_SHR
import org.vitrivr.cottontail.dbms.column.FixedLengthColumn.Companion.TABLET_SIZE
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.storage.serializers.SerializerFactory
import org.vitrivr.cottontail.storage.serializers.tablets.TabletSerializer

/**
 * A [Cursor] to iterate over the [Tablet]s of a [Column].
 */
class FixedLengthCursor<T: Value>(column: FixedLengthColumn<T>.Tx): Cursor<T?> {

    /** The Xodus transaction snapshot used by this FixedLengthCursor. */
    private val xodusTx = column.context.txn.xodusTx.readonlySnapshot

    /** Internal data [Store] reference. */
    private val store: Store = this.xodusTx.environment.openStore(
        column.dbo.name.storeName(),
        StoreConfig.USE_EXISTING,
        this.xodusTx,
        false
    ) ?: throw DatabaseException.DataCorruptionException("Data store for column ${column.dbo.name} is missing.")


    /** The internal [TabletSerializer] reference used for de-/serialization. */
    private val serializer: TabletSerializer<T> = SerializerFactory.tablet(column.columnDef.type, TABLET_SIZE, column.dbo.compression)

    /** Internal Xodus cursor instance.  */
    private val cursor = this.store.openCursor(this.xodusTx)

    /** The [TabletId] of the currently loaded [Tablet]. -1 if no [Tablet] has been loaded. */
    private var tupleId: TupleId = -1

    /** The [TabletId] of the currently loaded [Tablet]. -1 if no [Tablet] has been loaded. */
    private var tabletId: TabletId = -1

    /** The [TabletId] of the currently loaded [Tablet]. -1 if no [Tablet] has been loaded. */
    private var tabletIndex: Int = -1

    /** The currently loaded [Tablet]. */
    private var tablet: Tablet<T>? = null

    override fun moveNext(): Boolean = this.moveTo(this.tupleId + 1)
    override fun movePrevious(): Boolean  = this.moveTo(this.tupleId - 1)

    override fun moveTo(tupleId: TupleId): Boolean {
        if (tupleId < 0) return false
        if (this.tupleId == tupleId) return true

        /* Adjust tabletId. */
        val tabletId = tupleId ushr TABLET_SHR
        val tabletIdx = (tupleId % TABLET_SIZE).toInt()
        if (tabletId != this.tabletId) {
            val tablet = this.cursor.getSearchKey(LongBinding.longToCompressedEntry(tabletId))
            if (tablet != null) {
                this.tablet = this.serializer.fromEntry(tablet)
                this.tabletId = tabletId
            } else {
                return false
            }
        }
        this.tabletIndex = tabletIdx
        return true
    }

    override fun key(): TupleId = this.tupleId
    override fun value(): T? = this.tablet!![this.tabletIndex]
    override fun close() {
        this.cursor.close()
        this.xodusTx.abort()
    }
}