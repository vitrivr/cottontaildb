package org.vitrivr.cottontail.dbms.column

import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.Tablet
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.Transaction
import org.vitrivr.cottontail.storage.serializers.SerializerFactory
import org.vitrivr.cottontail.storage.serializers.tablets.TabletSerializer

/**
 *
 */
class FixedLengthCursor<T: Value>(private val column: Column<T>, private val transaction: Transaction): Cursor<T> {
    /** Internal data [Store] reference. */
    private val dataStore: Store = this.column.catalogue.transactionManager.environment.openStore(
        this.column.name.storeName(),
        StoreConfig.USE_EXISTING,
        this.transaction.xodusTx,
        false
    ) ?: throw DatabaseException.DataCorruptionException("Data store for column ${this.column.name} is missing.")


    /** The internal [TabletSerializer] reference used for de-/serialization. */
    private val serializer: TabletSerializer<T> = SerializerFactory.tablet(this.column.columnDef.type)

    /** Internal Xodus cursor instance.  */
    private val cursor = this.dataStore.openCursor(this.transaction.xodusTx)

    /** The [TabletId] of the currently loaded [Tablet]. -1 if no [Tablet] has been loaded. */
    private var tupleId: TabletId = -1

    /** The [TabletId] of the currently loaded [Tablet]. -1 if no [Tablet] has been loaded. */
    private var tabletIndex: Int = -1

    /** The currently loaded [Tablet]. */
    private var tablet: Tablet<T>? = null


    override fun moveNext(): Boolean {
        do {
            this.tupleId += this.tupleId + 1L
            this.tabletIndex = ((++this.tupleId) % Long.SIZE_BITS).toInt()
            if (this.tabletIndex == 0 && this.cursor.next) {
                 if (this.cursor.next) {
                    this.tablet = this.serializer.fromEntry(this.cursor.value)
                     return true
                } else {
                     return false
                }
            } else if (this.tabletIndex > 0 && this.tablet!![this.tabletIndex] != null) {
                return true
            }
        } while (true)
    }

    override fun key(): TupleId = this.tupleId
    override fun value(): T = this.tablet!![this.tabletIndex] ?: throw NoSuchElementException("")
    override fun close() = this.cursor.close()
}