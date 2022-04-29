package org.vitrivr.cottontail.dbms.column

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.BOC
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.entries.StatisticsCatalogueEntry
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.events.ColumnEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.exceptions.TransactionException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.general.AbstractTx
import org.vitrivr.cottontail.dbms.general.DBOVersion
import org.vitrivr.cottontail.dbms.statistics.columns.ValueStatistics
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

    /** Status indicating whether this [DefaultColumn] has been closed. */
    override val closed: Boolean
        get() = this.parent.closed

    /** A [DefaultColumn] belongs to the same [DefaultCatalogue] as the [DefaultEntity] it belongs to. */
    override val catalogue: DefaultCatalogue
        get() = this.parent.catalogue

    /** The [DBOVersion] of this [DefaultColumn]. */
    override val version: DBOVersion
        get() = DBOVersion.V3_0

    /**
     * Creates and returns a new [DefaultColumn.Tx] for the given [TransactionContext].
     *
     * @param context The [TransactionContext] to create the [DefaultColumn.Tx] for.
     * @return New [DefaultColumn.Tx]
     */
    override fun newTx(context: TransactionContext): ColumnTx<T> = Tx(context)

    /**
     *
     */
    override fun close() {/* No op. */ }

    /**
     * A [Tx] that affects this [DefaultColumn].
     */
    inner class Tx constructor(context: TransactionContext) : AbstractTx(context), ColumnTx<T> {

        /** Internal data [Store] reference. */
        private var dataStore: Store = this@DefaultColumn.catalogue.environment.openStore(
            this@DefaultColumn.name.storeName(),
            StoreConfig.USE_EXISTING,
            this.context.xodusTx,
            false
        ) ?: throw DatabaseException.DataCorruptionException("Data store for column ${this@DefaultColumn.name} is missing.")

        /** The internal [XodusBinding] reference used for de-/serialization. */
        private val binding: XodusBinding<T> = ValueSerializerFactory.xodus(this@DefaultColumn.columnDef.type, this@DefaultColumn.nullable)

        /** Internal reference to the [ValueStatistics] for this [DefaultColumn]. */
        @Suppress("UNCHECKED_CAST")
        private var statistics: ValueStatistics<T> = (StatisticsCatalogueEntry.read(this@DefaultColumn.name, this@DefaultColumn.catalogue, this.context.xodusTx)?.statistics
            ?: throw DatabaseException.DataCorruptionException("Failed to PUT value from ${this@DefaultColumn.name}: Reading column statistics failed.")) as ValueStatistics<T>

        /** Reference to [Column] this [Tx] belongs to. */
        override val dbo: DefaultColumn<T>
            get() = this@DefaultColumn

        /** Flag indicating that changes have been made through this [DefaultColumn.Tx] */
        @Volatile
        private var updateStatistics: Boolean = false

        /**
         * Obtains a global (non-exclusive) read-lock on [DefaultCatalogue].
         *
         * Prevents [DefaultCatalogue] from being closed while transaction is ongoing.
         */
        private val closeStamp: Long

        init {
            /** Checks if DBO is still open. */
            if (this.dbo.closed) {
                throw TransactionException.DBOClosed(this.context.txId, this.dbo)
            }
            this.closeStamp = this.dbo.catalogue.closeLock.readLock()
        }

        /**
         * Performs analysis of the [DefaultColumn] backing this [ColumnTx] and updates the associated [ValueStatistics].
         */
        @Suppress("UNCHECKED_CAST")
        override fun analyse() = this.txLatch.withLock {
            /* Reset and refresh statistics object. */
            val cursor = this.cursor()
            val newEntry = StatisticsCatalogueEntry(this@DefaultColumn.columnDef)
            while (cursor.moveNext()) {
                (newEntry.statistics as ValueStatistics<T>).insert(cursor.value())
            }

            /* Update statistics entry.*/
            if (StatisticsCatalogueEntry.write(newEntry, this@DefaultColumn.catalogue, this.context.xodusTx)) {
                this.statistics = StatisticsCatalogueEntry(this@DefaultColumn.columnDef).statistics as ValueStatistics<T>
                this.updateStatistics = false
            }

            /* Close cursor. */
            cursor.close()
        }

        /**
         * Gets and returns [ValueStatistics] for this [ColumnTx]
         *
         * @return [ValueStatistics].
         */
        override fun statistics(): ValueStatistics<T> = this.txLatch.withLock {
            this.statistics
        }

        /**
         * Returns the smallest [TupleId] held by the [Column] backing this [ColumnTx].
         *
         * @return [TupleId] The smallest [TupleId] held by the [Column] backing this [ColumnTx].
         */
        override fun smallestTupleId(): TupleId = this.txLatch.withLock {
            val cursor = this.dataStore.openCursor(this.context.xodusTx)
            val ret = if (cursor.next) {
                LongBinding.compressedEntryToLong(cursor.key)
            } else {
                BOC
            }
            cursor.close()
            ret
        }

        /**
         * Returns the largest [TupleId] held by the [Column] backing this [Tx].
         *
         * @return [TupleId] The largest [TupleId] held by the [Column] backing this [Tx].
         */
        override fun largestTupleId(): TupleId = this.txLatch.withLock {
            val cursor = this.dataStore.openCursor(this.context.xodusTx)
            val ret = if (cursor.last) {
                LongBinding.compressedEntryToLong(cursor.key)
            } else {
                BOC
            }
            cursor.close()
            ret
        }

        /**
         * Returns the number of entries in this [DefaultColumn].
         *
         * @return Number of entries in this [DefaultColumn].
         */
        override fun count(): Long  = this.txLatch.withLock { this.dataStore.count(this.context.xodusTx) }

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
            this.dataStore.get(this.context.xodusTx, tupleId.toKey()) != null
        }

        /**
         * Gets and returns an entry from this [Column].
         *
         * @param tupleId The ID of the desired entry
         * @return The desired entry.
         * @throws DatabaseException If the tuple with the desired ID is invalid.
         */
        override fun get(tupleId: TupleId): T? = this.txLatch.withLock {
            val ret = this.dataStore.get(this.context.xodusTx, tupleId.toKey()) ?: throw java.lang.IllegalArgumentException("Tuple $tupleId does not exist on column ${this@DefaultColumn.name}.")
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
            if (!this.dataStore.add(this.context.xodusTx, rawTuple, valueRaw)) {
                throw DatabaseException.DataCorruptionException("Failed to ADD tuple $tupleId to column ${this@DefaultColumn.name}.")
            }
            this.updateStatistics = true
            this.statistics.insert(value)

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
            val existingRaw = this.dataStore.get(this.context.xodusTx, rawTuple) ?: throw IllegalArgumentException("Cannot update tuple $tupleId because it does not exist.")
            val existing = this.binding.entryToValue(existingRaw)

            /* Perform PUT and update statistics. */
            if (!this.dataStore.put(this.context.xodusTx, rawTuple, valueRaw)) {
                throw DatabaseException.DataCorruptionException("Failed to PUT tuple $tupleId to column ${this@DefaultColumn.name}.")
            }
            this.updateStatistics = true
            this.statistics.update(existing, value)

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
            val existingRaw = this.dataStore.get(this.context.xodusTx, rawTuple) ?: throw IllegalArgumentException("Cannot DELETE tuple $tupleId because it does not exist.")
            val existing = this.binding.entryToValue(existingRaw)

            /* Delete entry and update statistics. */
            if (!this.dataStore.delete(this.context.xodusTx, rawTuple)) {
                throw DatabaseException.DataCorruptionException("Failed to DELETE tuple $tupleId to column ${this@DefaultColumn.name}.")
            }
            this.updateStatistics = true
            this.statistics.delete(existing)

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
            object : Cursor<T?> {

                /** The per-[Cursor] [XodusBinding] instance. */
                private val binding: XodusBinding<T> = ValueSerializerFactory.xodus(this@DefaultColumn.columnDef.type, this@DefaultColumn.nullable)

                /** Creates a read-only snapshot of the enclosing Tx. */
                private val subTransaction = this@Tx.context.xodusTx.readonlySnapshot

                /** Internal [Cursor] used for iteration. */
                private val cursor: jetbrains.exodus.env.Cursor = this@Tx.dataStore.openCursor(this.subTransaction)

                /** The [TupleId] this [Cursor] is currently pointing to. -1L is equivalent to BOF. */
                private var tupleId: TupleId = -1L

                /** The [TupleId] this [Cursor] is currently pointing to. -1L is equivalent to BOF. */
                private var value: T? = null

                /** Flag indicating, that data must be read from store. */
                private var dirty: Boolean = true

                /**
                 * Tries to move this [Cursor] to the next entry.
                 *
                 * @return True on success, false otherwise,
                 */
                override fun moveNext(): Boolean {
                    check(!this.subTransaction.isFinished) { "Cursor cannot be moved because associated transaction has completed!" }
                    this.dirty = if (this.tupleId == BOC) {
                        if (partition.first == BOC) return false
                        this.cursor.getSearchKeyRange(partition.first.toKey()) != null
                    } else {
                        this.cursor.next
                    }
                    if (this.dirty) {
                        this.tupleId = LongBinding.compressedEntryToLong(this.cursor.key)
                    }
                    return this.dirty && this.tupleId <= partition.last
                }

                /**
                 *
                 */
                override fun key(): TupleId = this.tupleId

                /**
                 *
                 */
                override fun value(): T? {
                    if (this.dirty) {
                        this.value = this.binding.entryToValue(this.cursor.value)
                        this.dirty = false
                    }
                    return this.value
                }

                /**
                 * Closes this [Cursor] and invalidates the associated sub transaction.
                 */
                override fun close() {
                    this.cursor.close()
                    this.subTransaction.abort()
                }
            }
        }

        /**
         * Called when a transactions commit. Updates [StatisticsCatalogueEntry].
         */
        override fun beforeCommit() {
            /* Update statistics if there have been changes. */
            if (this.updateStatistics) {
                val entry = StatisticsCatalogueEntry.read(this@DefaultColumn.name, this@DefaultColumn.catalogue, this.context.xodusTx)
                    ?: throw DatabaseException.DataCorruptionException("Failed to finalize transaction for column ${this@DefaultColumn.name}: Reading column statistics failed.")
                StatisticsCatalogueEntry.write(entry.copy(statistics = this.statistics), this@DefaultColumn.catalogue, this.context.xodusTx)

                /* If statistics are no longer fresh, then issue event. */
                if (!this.statistics.fresh) {
                    this.context.signalEvent(ColumnEvent.Stale(this@DefaultColumn.name))
                }
            }
            super.beforeCommit()
        }

        /**
         * Called when a transaction finalizes. Releases the lock held on the [DefaultCatalogue].
         */
        override fun cleanup() {
            this.dbo.catalogue.closeLock.unlockRead(this.closeStamp)
        }
    }
}