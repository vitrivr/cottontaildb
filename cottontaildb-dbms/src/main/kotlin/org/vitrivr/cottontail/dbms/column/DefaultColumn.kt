package org.vitrivr.cottontail.dbms.column

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.BOC
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.events.ColumnEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.general.AbstractTx
import org.vitrivr.cottontail.dbms.general.DBOVersion
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.statistics.columns.ColumnStatistic
import org.vitrivr.cottontail.dbms.statistics.values.ValueStatistics
import org.vitrivr.cottontail.storage.serializers.values.ValueSerializerFactory
import org.vitrivr.cottontail.storage.serializers.values.xodus.XodusBinding
import kotlin.concurrent.withLock

/**
 * The default [ColumnDef] implementation based on JetBrains Xodus.
 *
 * @see Column
 * @see ColumnTx
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class DefaultColumn<T : Value>(override val columnDef: ColumnDef<T>, override val parent: DefaultEntity) : Column<T> {

    /** The [Name.ColumnName] of this [DefaultColumn]. */
    override val name: Name.ColumnName
        get() = this.columnDef.name

    /** A [DefaultColumn] belongs to the same [DefaultCatalogue] as the [DefaultEntity] it belongs to. */
    override val catalogue: DefaultCatalogue
        get() = this.parent.catalogue

    /** The [DBOVersion] of this [DefaultColumn]. */
    override val version: DBOVersion
        get() = DBOVersion.V3_0

    /**
     * Creates and returns a new [DefaultColumn.Tx] for the given [QueryContext].
     *
     * @param context The [QueryContext] to create the [DefaultColumn.Tx] for.
     * @return New [DefaultColumn.Tx]
     */
    override fun newTx(context: QueryContext): ColumnTx<T>
        = context.txn.getCachedTxForDBO(this) ?: this.Tx(context)

    override fun equals(other: Any?): Boolean {
        if (other !is DefaultColumn<*>) return false
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
     * A [Tx] that affects this [DefaultColumn].
     */
    inner class Tx constructor(context: QueryContext) : AbstractTx(context), ColumnTx<T>, org.vitrivr.cottontail.dbms.general.Tx.WithCommitFinalization  {


        init {
            /* Cache this Tx for future use. */
            context.txn.cacheTxForDBO(this)
        }

        /** Internal data [Store] reference. */
        internal val dataStore: Store = this@DefaultColumn.catalogue.environment.openStore(
            this@DefaultColumn.name.storeName(),
            StoreConfig.USE_EXISTING,
            this.context.txn.xodusTx,
            false
        ) ?: throw DatabaseException.DataCorruptionException("Data store for column ${this@DefaultColumn.name} is missing.")

        /** The internal [XodusBinding] reference used for de-/serialization. */
        internal val binding: XodusBinding<T> = ValueSerializerFactory.xodus(this@DefaultColumn.columnDef.type, this@DefaultColumn.nullable)

        /** Reference to [Column] this [Tx] belongs to. */
        override val dbo: DefaultColumn<T>
            get() = this@DefaultColumn

        /**
         * Performs analysis of the [DefaultColumn] backing this [ColumnTx] and updates the associated [ValueStatistics].
         */
        @Suppress("UNCHECKED_CAST")
        override fun analyse() = this.txLatch.withLock {
            /* Reset and refresh statistics object. */
            val cursor = this.cursor()
            val newEntry = ColumnStatistic(this@DefaultColumn.columnDef)
            while (cursor.moveNext()) {
                (newEntry.statistics as ValueStatistics<T>).insert(cursor.value())
            }
            cursor.close()

            /* Update statistics entry.*/
            this@DefaultColumn.catalogue.columnStatistics.update(newEntry)
        }

        /**
         * Gets and returns [ValueStatistics] for this [ColumnTx]
         *
         * @return [ValueStatistics].
         */
        @Suppress("UNCHECKED_CAST")
        override fun statistics(): ValueStatistics<T> = this.txLatch.withLock {
            (this@DefaultColumn.catalogue.columnStatistics[this@DefaultColumn.name]?.statistics
                ?: throw DatabaseException.DataCorruptionException("Failed to read column statistics.")) as ValueStatistics<T>
        }

        /**
         * Returns the smallest [TupleId] held by the [Column] backing this [ColumnTx].
         *
         * @return [TupleId] The smallest [TupleId] held by the [Column] backing this [ColumnTx].
         */
        override fun smallestTupleId(): TupleId = this.txLatch.withLock {
            this.dataStore.openCursor(this.context.txn.xodusTx).use {
                if (it.next) {
                    LongBinding.compressedEntryToLong(it.key)
                } else {
                    BOC
                }
            }
        }

        /**
         * Returns the largest [TupleId] held by the [Column] backing this [Tx].
         *
         * @return [TupleId] The largest [TupleId] held by the [Column] backing this [Tx].
         */
        override fun largestTupleId(): TupleId = this.txLatch.withLock {
            this.dataStore.openCursor(this.context.txn.xodusTx).use {
                if (it.last) {
                    LongBinding.compressedEntryToLong(it.key)
                } else {
                    BOC
                }
            }
        }

        /**
         * Returns the number of entries in this [DefaultColumn].
         *
         * @return Number of entries in this [DefaultColumn].
         */
        override fun count(): Long  = this.txLatch.withLock { this.dataStore.count(this.context.txn.xodusTx) }

        /**
         * Returns true if the [Column] underpinning this [ColumnTx] contains the given [TupleId] and false otherwise.
         *
         * This method checks the existence of the [TupleId] within the [Column] If this method returns true,
         * then [ColumnTx.get] will either return a [Value] or null. However, if this method returns false, then
         * [ColumnTx.get] will throw an exception for that [TupleId].
         *
         * @param tupleId The [TupleId] of the desired entry
         * @return True if entry exists, false otherwise,
         */
        override fun contains(tupleId: TupleId): Boolean = this.txLatch.withLock {
            this.dataStore.get(this.context.txn.xodusTx, tupleId.toKey()) != null
        }

        /**
         * Gets and returns an entry from this [Column].
         *
         * @param tupleId The ID of the desired entry
         * @return The desired entry.
         * @throws DatabaseException If the tuple with the desired ID is invalid.
         */
        override fun get(tupleId: TupleId): T? = this.txLatch.withLock {
            val ret = this.dataStore.get(this.context.txn.xodusTx, tupleId.toKey()) ?: throw java.lang.IllegalArgumentException("Tuple $tupleId does not exist on column ${this@DefaultColumn.name}.")
            return this.binding.entryToValue(ret)
        }

        /**
         * Inserts the [Value] with the specified [TupleId].
         *
         * @param tupleId The [TupleId] of the entry that should be updated.
         * @param value The [Value]
         * @return True on success, false otherwise.
         */
        override fun add(tupleId: TupleId, value: T?): Boolean = this.txLatch.withLock {
            val rawTuple = tupleId.toKey()
            val valueRaw = this.binding.valueToEntry(value)
            if (!this.dataStore.add(this.context.txn.xodusTx, rawTuple, valueRaw)) {
                throw DatabaseException.DataCorruptionException("Failed to ADD tuple $tupleId to column ${this@DefaultColumn.name}.")
            }
            this.statistics().insert(value)

            /* Return success status. */
            return true
        }

        /**
         * Updates the entry with the specified [TupleId] and sets it to the new [Value].
         *
         * @param tupleId The [TupleId] of the entry that should be updated.
         * @param value The new [Value]
         * @return The old [Value]
         */
        override fun update(tupleId: TupleId, value: T?): T? = this.txLatch.withLock {
            /* Read existing value. */
            val rawTuple = tupleId.toKey()
            val valueRaw = this.binding.valueToEntry(value)
            val existingRaw = this.dataStore.get(this.context.txn.xodusTx, rawTuple) ?: throw IllegalArgumentException("Cannot update tuple $tupleId because it does not exist.")
            val existing = this.binding.entryToValue(existingRaw)

            /* Perform PUT and update statistics. */
            if (!this.dataStore.put(this.context.txn.xodusTx, rawTuple, valueRaw)) {
                throw DatabaseException.DataCorruptionException("Failed to PUT tuple $tupleId to column ${this@DefaultColumn.name}.")
            }
            this.statistics().update(existing, value)

            /* Return updated value. */
            return existing
        }

        /**
         * Deletes the entry with the specified [TupleId] and sets it to the new value.
         *
         * @param tupleId The ID of the record that should be updated
         * @return The old [Value]
         */
        override fun delete(tupleId: TupleId): T? = this.txLatch.withLock {
            /* Read existing value. */
            val rawTuple = tupleId.toKey()
            val existingRaw = this.dataStore.get(this.context.txn.xodusTx, rawTuple) ?: throw IllegalArgumentException("Cannot DELETE tuple $tupleId because it does not exist.")
            val existing = this.binding.entryToValue(existingRaw)

            /* Delete entry and update statistics. */
            if (!this.dataStore.delete(this.context.txn.xodusTx, rawTuple)) {
                throw DatabaseException.DataCorruptionException("Failed to DELETE tuple $tupleId to column ${this@DefaultColumn.name}.")
            }
            this.statistics().delete(existing)

            /* Return existing value. */
            return existing
        }

        /**
         * Opens a new [Cursor] for this [DefaultColumn.Tx].
         *
         * @return [Cursor]
         */
        override fun cursor(): Cursor<T?> = this.txLatch.withLock {
            this.cursor(this.smallestTupleId()..this.largestTupleId())
        }

        /**
         * Opens a new [Cursor] for this [DefaultColumn.Tx].
         *
         * @param partition The [LongRange] specifying the [TupleId]s that should be scanned.
         * @return [Cursor]
         */
        override fun cursor(partition: LongRange): Cursor<T?> = this.txLatch.withLock {
            DefaultColumnCursor(partition, this, this.context)
        }

        /**
         * Called when a transactions commit.
         *
         * Checks for freshness of [ColumnStatistic].
         */
        override fun beforeCommit() {
            if (!this.statistics().fresh) {
                this.context.txn.signalEvent(ColumnEvent.Stale(this@DefaultColumn.name))
            }
        }
    }
}