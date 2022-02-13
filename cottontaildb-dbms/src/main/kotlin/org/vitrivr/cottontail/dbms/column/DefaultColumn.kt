package org.vitrivr.cottontail.dbms.column

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.entries.StatisticsCatalogueEntry
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.exceptions.TxException
import org.vitrivr.cottontail.dbms.execution.TransactionContext
import org.vitrivr.cottontail.dbms.general.AbstractTx
import org.vitrivr.cottontail.dbms.general.DBOVersion
import org.vitrivr.cottontail.dbms.statistics.columns.ValueStatistics
import org.vitrivr.cottontail.storage.serializers.ValueSerializerFactory
import org.vitrivr.cottontail.storage.serializers.xodus.XodusBinding
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
        private val statistics: ValueStatistics<T> = (StatisticsCatalogueEntry.read(this@DefaultColumn.name, this@DefaultColumn.catalogue, this.context.xodusTx)?.statistics
            ?: throw DatabaseException.DataCorruptionException("Failed to PUT value from ${this@DefaultColumn.name}: Reading column statistics failed.")) as ValueStatistics<T>

        /** Reference to [Column] this [Tx] belongs to. */
        override val dbo: DefaultColumn<T>
            get() = this@DefaultColumn

        /**
         * Obtains a global (non-exclusive) read-lock on [DefaultCatalogue].
         *
         * Prevents [DefaultCatalogue] from being closed while transaction is ongoing.
         */
        private val closeStamp = this.dbo.catalogue.closeLock.readLock()

        /** Flag indicating that changes have been made through this [DefaultColumn.Tx] */
        @Volatile
        private var dirty: Boolean = false

        init {
            /** Checks if DBO is still open. */
            if (this.dbo.closed) {
                this.dbo.catalogue.closeLock.unlockRead(this.closeStamp)
                throw TxException.TxDBOClosedException(this.context.txId, this.dbo)
            }

           this@DefaultColumn.catalogue.environment.statistics
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
         * Returns the number of entries in this [DefaultColumn].
         *
         * @return Number of entries in this [DefaultColumn].
         */
        override fun count(): Long  = this.txLatch.withLock { this.dataStore.count(this.context.xodusTx) }

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
        override fun add(tupleId: TupleId, value: T?): Boolean {
            val rawTuple = tupleId.toKey()
            val valueRaw = this.binding.valueToEntry(value)
            if (this.dataStore.add(this.context.xodusTx, rawTuple, valueRaw)) {
                this.dirty = true
                this.statistics.insert(value)
                return true
            }
            return false
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
            this.dirty = true
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
            this.dirty = true
            this.statistics.delete(existing)

            /* Return existing value. */
            return existing
        }

        /**
         * Opens a new [Cursor] for this [DefaultColumn.Tx].
         *
         * @return [Cursor]
         */
        override fun cursor(): Cursor<T?> = this.cursor(-1L, Long.MAX_VALUE)

        /**
         * Opens a new [Cursor] for this [DefaultColumn.Tx].
         *
         * @param start The [TupleId] to start the [Cursor] at.
         * @return [Cursor]
         */
        override fun cursor(start: TupleId, end: TupleId): Cursor<T?> = this.txLatch.withLock {
            object : Cursor<T?> {

                /** Creates a read-only snapshot of the enclosing Tx. */
                private val subTx = this@Tx.context.xodusTx.readonlySnapshot

                /** The per-[Cursor] [XodusBinding] instance. */
                private val binding: XodusBinding<T> = ValueSerializerFactory.xodus(this@DefaultColumn.columnDef.type, this@DefaultColumn.nullable)

                /** Internal [Cursor] used for iteration. */
                private val cursor: jetbrains.exodus.env.Cursor = this@Tx.dataStore.openCursor(this.subTx)

                /** Serialize the start value to a [ArrayByteIterable]. */
                private val end: ArrayByteIterable = end.toKey()

                init {
                    if (start > -1L) this.cursor.getSearchKeyRange(start.toKey())
                }

                override fun moveNext(): Boolean = this.cursor.next && this.cursor.key < this.end
                override fun key(): TupleId = LongBinding.compressedEntryToLong(this.cursor.key)
                override fun value(): T? = this.binding.entryToValue(this.cursor.value)
                override fun close() {
                    this.cursor.close()
                    this.subTx.abort()
                }
            }
        }

        /**
         * Called when a transactions commits. Updates [StatisticsCatalogueEntry].
         */
        override fun beforeCommit() {
            /* Update statistics if there have been changes. */
            if (this.dirty) {
                val entry = StatisticsCatalogueEntry.read(this@DefaultColumn.name, this@DefaultColumn.catalogue, this.context.xodusTx)
                    ?: throw DatabaseException.DataCorruptionException("Failed to DELETE value from ${this@DefaultColumn.name}: Reading column statistics failed.")
                StatisticsCatalogueEntry.write(entry.copy(statistics = this.statistics), this@DefaultColumn.catalogue, this.context.xodusTx)
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