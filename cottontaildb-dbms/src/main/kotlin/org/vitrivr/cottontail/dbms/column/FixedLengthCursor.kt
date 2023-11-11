package org.vitrivr.cottontail.dbms.column

import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.values.Value
import org.vitrivr.cottontail.core.values.tablets.Tablet
import org.vitrivr.cottontail.core.values.tablets.bytebuffer.AbstractByteBufferTablet
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.storage.serializers.SerializerFactory
import org.vitrivr.cottontail.storage.serializers.tablets.TabletSerializer

/**
 *
 */
class FixedLengthCursor<T: Value>(tx: FixedLengthColumn<T>.Tx): Cursor<T> {

    /** The Xodus [jetbrains.exodus.env.Transaction]. */
    private val xodusTx: jetbrains.exodus.env.Transaction = tx.xodusTx.readonlySnapshot

    /** Internal data [Store] reference. */
    private val store: Store = this.xodusTx.environment.openStore(tx.dbo.name.storeName(), StoreConfig.USE_EXISTING, this.xodusTx, false)
        ?: throw DatabaseException.DataCorruptionException("Data store for column ${tx.dbo.name.storeName()} is missing.")

    /** The internal [TabletSerializer] reference used for de-/serialization. */
    private val serializer: TabletSerializer<T> = SerializerFactory.tablet(tx.dbo.columnDef.type, 128)

    /** Internal Xodus cursor instance.  */
    private val cursor = this.store.openCursor(this.xodusTx)

    /** The [TabletId] of the currently loaded [Tablet]. -1 if no [Tablet] has been loaded. */
    private var tupleId: TabletId = -1

    /** The [TabletId] of the currently loaded [Tablet]. -1 if no [Tablet] has been loaded. */
    private var tabletIndex: Int = -1

    /** The currently loaded [AbstractByteBufferTablet]. */
    private var tablet: AbstractByteBufferTablet<T>? = null

    override fun moveNext(): Boolean {
        do {
            this.tupleId += this.tupleId + 1L
            this.tabletIndex = ((++this.tupleId) % Long.SIZE_BITS).toInt()
            if (this.tabletIndex == 0 && this.cursor.next) {
                return if (this.cursor.next) {
                    this.tablet = this.serializer.fromEntry(this.cursor.value)
                    true
                } else {
                    false
                }
            } else if (this.tabletIndex > 0 && this.tablet!![this.tabletIndex] != null) {
                return true
            }
        } while (true)
    }

    override fun key(): TupleId = this.tupleId
    override fun value(): T = this.tablet!![this.tabletIndex] ?: throw NoSuchElementException("")
    override fun close() {
        this.cursor.close()
        this.xodusTx.abort()
    }
}