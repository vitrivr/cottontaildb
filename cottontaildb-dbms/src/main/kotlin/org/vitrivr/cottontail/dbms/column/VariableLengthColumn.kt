package org.vitrivr.cottontail.dbms.column

import it.unimi.dsi.fastutil.longs.Long2ObjectFunction
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.column.ColumnMetadata.Companion.storeName
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.schema.DefaultSchema
import org.vitrivr.cottontail.dbms.statistics.defaultStatistics
import org.vitrivr.cottontail.dbms.statistics.values.ValueStatistics
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
 * @version 5.0.0
 */
class VariableLengthColumn<T : Value>(override val columnDef: ColumnDef<T>, override val parent: DefaultEntity) : Column<T> {

    /** The [Name.ColumnName] of this [VariableLengthColumn]. */
    override val name: Name.ColumnName
        get() = this.columnDef.name

    /** A [VariableLengthColumn] belongs to the same [DefaultCatalogue] as the [DefaultEntity] it belongs to. */
    override val catalogue: DefaultCatalogue
        get() = this.parent.catalogue

    /** An internal cache of all ongoing [DefaultSchema.Tx]s for this [DefaultSchema]. */
    private val transactions = Long2ObjectOpenHashMap<VariableLengthColumn<T>.Tx>()

    /**
     * Creates and returns a new [VariableLengthColumn.Tx] for the given [EntityTx].
     *
     * @param parent The parent [EntityTx] to create the [VariableLengthColumn.Tx] for.
     * @return New [VariableLengthColumn.Tx]
     */
    override fun newTx(parent: EntityTx): ColumnTx<T> {
        require(parent is DefaultEntity.Tx) { "VariableLengthColumns can only be accessed with a DefaultEntity.Tx!" }
        return this.transactions.computeIfAbsent(parent.context.txn.transactionId, Long2ObjectFunction {
            val subTransaction = Tx(parent)
            parent.context.txn.registerSubtransaction(subTransaction)
            subTransaction
        })
    }

    /**
     * A [Tx] that affects this [VariableLengthColumn].
     */
    inner class Tx(override val parent: DefaultEntity.Tx): ColumnTx<T>  {

        /** Reference to the surrounding entities Xodus transaction. */
        internal val xodusTx = this.parent.xodusTx

        /** Internal data [Store] reference. */
        private val dataStore: Store = this.xodusTx.environment.openStore(
            this@VariableLengthColumn.name.storeName(),
            StoreConfig.WITHOUT_DUPLICATES,
            this.xodusTx
        )

        /** The internal [ValueSerializer] reference used for de-/serialization. */
        private val binding: ValueSerializer<T> = SerializerFactory.value(this@VariableLengthColumn.columnDef.type)

        /** Reference to [Column] this [Tx] belongs to. */
        override val dbo: VariableLengthColumn<T>
            get() = this@VariableLengthColumn

        /**
         * Gets and returns [ValueStatistics] for this [ColumnTx]
         *
         * @return [ValueStatistics].
         */
        @Suppress("UNCHECKED_CAST")
        @Synchronized
        override fun statistics(): ValueStatistics<T> {
            val statistics = this.context.statistics[this@VariableLengthColumn.name]?.statistics as? ValueStatistics<T>
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
        @Synchronized
        override fun read(tupleId: TupleId): T? {
            val ret = this.dataStore.get(this.xodusTx, tupleId.toKey()) ?: return null
            return this.binding.fromEntry(ret)
        }

        /**
         * Inserts the [Value] with the specified [TupleId].
         *
         * @param tupleId The [TupleId] of the entry that should be updated.
         * @param value The [Value]
         * @return True on success, false otherwise.
         */
        @Synchronized
        override fun write(tupleId: TupleId, value: T): T? {
            val tupleIdRaw = tupleId.toKey()
            val valueRaw = this.binding.toEntry(value)
            val oldRaw = this.dataStore.get(this.xodusTx, tupleIdRaw)

            /* Update value. */
            this.dataStore.put(this.xodusTx, tupleIdRaw, valueRaw)

            /* Return previous value. */
            return oldRaw?.let { this.binding.fromEntry(it)  }
        }

        /**
         * Deletes the [Value] entry with the specified [TupleId] .
         *
         * @param tupleId The [TupleId] of the entry that should be deleted.
         * @return The old [Value]
         */
        @Synchronized
        override fun delete(tupleId: TupleId): T? {
            val tupleIdRaw = tupleId.toKey()
            val oldRaw = this.dataStore.get(this.xodusTx, tupleIdRaw)

            /* Delete value. */
            this.dataStore.delete(this.xodusTx, tupleIdRaw)

            /* Return previous value. */
            return oldRaw?.let { this.binding.fromEntry(it) }
        }

        /**
         * Returns a [Cursor] that can be used to iterate over all entries in this [Column].
         *
         * @return [Cursor]
         */
        @Suppress("UNCHECKED_CAST")
        @Synchronized
        override fun cursor(): Cursor<T?> {
            val count = this.dataStore.count(this.xodusTx)
            if (count == 0L) return EmptyColumnCursor as Cursor<T?>
            return VariableLengthCursor(this)
        }
    }
}