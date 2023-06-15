package org.vitrivr.cottontail.dbms.column

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.general.AbstractTx
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.statistics.defaultStatistics
import org.vitrivr.cottontail.dbms.statistics.values.*
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
class VariableLengthColumn<T : Value>(override val columnDef: ColumnDef<T>, override val parent: DefaultEntity) : Column<T> {

    /** The [Name.ColumnName] of this [VariableLengthColumn]. */
    override val name: Name.ColumnName
        get() = this.columnDef.name

    /** A [VariableLengthColumn] belongs to the same [DefaultCatalogue] as the [DefaultEntity] it belongs to. */
    override val catalogue: DefaultCatalogue
        get() = this.parent.catalogue

    /**
     * Creates and returns a new [VariableLengthColumn.Tx] for the given [QueryContext].
     *
     * @param context The [QueryContext] to create the [VariableLengthColumn.Tx] for.
     * @return New [VariableLengthColumn.Tx]
     */
    override fun newTx(context: QueryContext): ColumnTx<T>
        = context.txn.getCachedTxForDBO(this) ?: this.Tx(context)

    override fun equals(other: Any?): Boolean {
        if (other !is VariableLengthColumn<*>) return false
        if (other.catalogue != this.catalogue) return false
        if (other.name != this.name) return false
        return true
    }

    override fun hashCode(): Int {
        var result = name.hashCode()
        result = 31 * result + parent.hashCode()
        return result
    }

    /**
     * A [Tx] that affects this [VariableLengthColumn].
     */
    inner class Tx constructor(context: QueryContext) : AbstractTx(context), ColumnTx<T>  {
        init {
            /* Cache this Tx for future use. */
            context.txn.cacheTx(this)
        }

        /** Internal data [Store] reference. */
        private val dataStore: Store = this@VariableLengthColumn.catalogue.transactionManager.environment.openStore(
            this@VariableLengthColumn.name.storeName(),
            StoreConfig.USE_EXISTING,
            this.context.txn.xodusTx,
            false
        ) ?: throw DatabaseException.DataCorruptionException("Data store for column ${this@VariableLengthColumn.name} is missing.")

        /** The internal [ValueSerializer] reference used for de-/serialization. */
        internal val binding: ValueSerializer<T> = SerializerFactory.value(this@VariableLengthColumn.columnDef.type, this@VariableLengthColumn.nullable)

        /** Reference to [Column] this [Tx] belongs to. */
        override val dbo: VariableLengthColumn<T>
            get() = this@VariableLengthColumn

        /**
         * Gets and returns [ValueStatistics] for this [ColumnTx]
         *
         * @return [ValueStatistics].
         */
        @Suppress("UNCHECKED_CAST")
        override fun statistics(): ValueStatistics<T> = this.txLatch.withLock {
            val statistics = this@VariableLengthColumn.catalogue.statisticsManager[this@VariableLengthColumn.name]?.statistics as? ValueStatistics<T>
            if (statistics != null) return statistics
            return type.defaultStatistics()
        }


        /**
         * Gets and returns an entry from this [Column].
         *
         * @param tupleId The ID of the desired entry
         * @return The desired entry.
         * @throws DatabaseException If the tuple with the desired ID is invalid.
         */
        override fun read(tupleId: TupleId): T? = this.txLatch.withLock {
            val ret = this.dataStore.get(this.context.txn.xodusTx, tupleId.toKey()) ?: return@withLock null
            return this.binding.fromEntry(ret)
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
            val valueRaw = this.binding.toEntry(value)
            val oldRaw = this.dataStore.get(this.context.txn.xodusTx, tupleId.toKey())

            /* Update value. */
            this.dataStore.put(this.context.txn.xodusTx, rawTuple, valueRaw)

            /* Return previous value. */
            if (oldRaw != null) {
                this.binding.fromEntry(oldRaw)
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
            val oldRaw = this.dataStore.get(this.context.txn.xodusTx, tupleId.toKey())

            /* Delete value. */
            this.dataStore.delete(this.context.txn.xodusTx, rawTupleId)

            /* Return previous value. */
            if (oldRaw != null) {
                this.binding.fromEntry(oldRaw)
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
            if (this.dataStore.count(this.context.txn.xodusTx) == 0L) return EmptyColumnCursor as Cursor<T>
            return VariableLengthCursor(this@VariableLengthColumn, this.context.txn)
        }
    }
}