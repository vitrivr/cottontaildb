package org.vitrivr.cottontail.dbms.column

import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.values.Value
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.Transaction
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.storage.serializers.SerializerFactory
import org.vitrivr.cottontail.storage.serializers.values.ValueSerializer
import kotlin.concurrent.withLock

/**
 * The default [ColumnDef] implementation based on JetBrains Xodus.
 *
 * @see Column
 * @see ColumnTx
 *
 * @author Ralph Gasser
 * @version 4.0.0
 */
class VariableLengthColumn<T : Value>(columnDef: ColumnDef<T>, parent: DefaultEntity) : DefaultColumn<T>(columnDef, parent) {

    /** The [Name.ColumnName] of this [VariableLengthColumn]. */
    override val name: Name.ColumnName
        get() = this.columnDef.name

    /** A [VariableLengthColumn] belongs to the same [DefaultCatalogue] as the [DefaultEntity] it belongs to. */
    override val catalogue: DefaultCatalogue
        get() = this.parent.catalogue

    /**
     * Creates and returns a new [VariableLengthColumn.Tx] for the given [QueryContext].
     *
     * @param parent The [Transaction] to create the [VariableLengthColumn.Tx] for.
     * @return New [VariableLengthColumn.Tx]
     */
    override fun newTx(parent: EntityTx): ColumnTx<T> {
        require(parent is DefaultEntity.Tx) { "VariableLengthColumn can only be used with DefaultEntity.Tx" }
        return this.Tx(parent)
    }

    /**
     * A [Tx] that affects this [VariableLengthColumn].
     */
    inner class Tx constructor(parent: DefaultEntity.Tx) : DefaultColumn<T>.Tx(parent) {

        /** Reference to [Column] this [Tx] belongs to. */
        override val dbo: VariableLengthColumn<T>
            get() = this@VariableLengthColumn

        /** The internal [ValueSerializer] reference used for de-/serialization. */
        private val serializer: ValueSerializer<T> = SerializerFactory.value(this@VariableLengthColumn.columnDef.type)

        /**
         * Gets and returns an entry from this [Column].
         *
         * @param tupleId The ID of the desired entry
         * @return The desired entry.
         * @throws DatabaseException If the tuple with the desired ID is invalid.
         */
        override fun read(tupleId: TupleId): T? = this.txLatch.withLock {
            val ret = this.store.get(this.xodusTx, tupleId.toKey()) ?: return@withLock null
            return this.serializer.fromEntry(ret)
        }

        /**
         * Inserts the [Value] with the specified [TupleId].
         *
         * @param tupleId The [TupleId] of the entry that should be updated.
         * @param value The [Value]
         * @return True on success, false otherwise.
         */
        override fun write(tupleId: TupleId, value: T): T? = this.txLatch.withLock {
            val rawTuple = tupleId.toKey()
            val valueRaw = this.serializer.toEntry(value)
            val oldRaw = this.store.get(this.xodusTx, tupleId.toKey())

            /* Update value. */
            this.store.put(this.xodusTx, rawTuple, valueRaw)

            /* Return previous value. */
            if (oldRaw != null) {
                this.serializer.fromEntry(oldRaw)
            } else {
                null
            }
        }

        /**
         * Deletes the [Value] entry with the specified [TupleId] .
         *
         * @param tupleId The [TupleId] of the entry that should be deleted.
         * @return The old [Value]
         */
        override fun delete(tupleId: TupleId): T? = this.txLatch.withLock {
            val rawTupleId = tupleId.toKey()
            val oldRaw = this.store.get(this.xodusTx, tupleId.toKey())

            /* Delete value. */
            this.store.delete(this.xodusTx, rawTupleId)

            /* Return previous value. */
            if (oldRaw != null) {
                this.serializer.fromEntry(oldRaw)
            } else {
                null
            }
        }

        /**
         * Returns a [Cursor] that can be used to iterate over all entries in this [Column].
         *
         * @return [Cursor]
         */
        @Suppress("UNCHECKED_CAST")
        override fun cursor(): Cursor<T> {
            if (this.store.count(this.xodusTx) == 0L) return EmptyColumnCursor as Cursor<T>
            return VariableLengthCursor(this)
        }
    }
}