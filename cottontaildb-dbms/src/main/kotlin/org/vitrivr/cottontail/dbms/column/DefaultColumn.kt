package org.vitrivr.cottontail.dbms.column

import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.core.values.UuidValue
import org.vitrivr.cottontail.core.values.Value
import org.vitrivr.cottontail.core.values.tablets.Tablet
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.Transaction
import org.vitrivr.cottontail.dbms.statistics.defaultStatistics
import org.vitrivr.cottontail.dbms.statistics.values.ValueStatistics
import java.util.*
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Abstract [Column] implementation for columns based on JetBrains Xodus.
 *
 * @see Column
 * @see ColumnTx
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class DefaultColumn<T : Value>(final override val columnDef: ColumnDef<T>, final override val parent: DefaultEntity) : Column<T> {
    /** The [Name.ColumnName] of this [DefaultColumn]. */
    override val name: Name.ColumnName
        get() = this.columnDef.name

    /** A [DefaultColumn] belongs to the same [DefaultCatalogue] as the [DefaultEntity] it belongs to. */
    override val catalogue: DefaultCatalogue
        get() = this.parent.catalogue

    /**
     * Compares this [DefaultColumn] to another object.
     */
    override fun equals(other: Any?): Boolean {
        if (other !is DefaultColumn<*>) return false
        if (other.catalogue != this.catalogue) return false
        if (other.name != this.name) return false
        return true
    }

    /**
     * Hash code for this [DefaultColumn].
     */
    override fun hashCode(): Int {
        var result = name.hashCode()
        result = 31 * result + parent.hashCode()
        return result
    }

    /**
     * A [Tx] that affects this [VariableLengthColumn].
     */
    abstract inner class Tx constructor(parent: DefaultEntity.Tx) : ColumnTx<T>, org.vitrivr.cottontail.dbms.general.Tx.WithCommitFinalization {
        /** Reference to the Cottontail DB [Transaction] object. */
        final override val transaction: Transaction = parent.transaction

        /** The Xodus [jetbrains.exodus.env.Transaction]. */
        internal val xodusTx: jetbrains.exodus.env.Transaction = parent.xodusTx

        /** Internal data [Store] reference. */
        protected var store: Store = this.xodusTx.environment.openStore(this@DefaultColumn.name.storeName(), StoreConfig.WITHOUT_DUPLICATES, this.xodusTx)

        /** Flag indicating, that the currently loaded [Tablet] has been changed. */
        @Volatile
        private var autoIncrementDirty = false

        /** The next auto-increment value. */
        @Volatile
        private var autoIncrementCurrent = 0L

        /** A [ReentrantLock] that synchronises access to this [Tx]'s methods. */
        protected val txLatch = ReentrantLock()

        init {
            if (this@DefaultColumn.columnDef.autoIncrement) {
                val columnMetadataStore = this.xodusTx.environment.openStore(DefaultCatalogue.COLUMN_METADATA_STORE_NAME, StoreConfig.USE_EXISTING, this.xodusTx, false)
                    ?: throw DatabaseException.DataCorruptionException("Failed to open store for column metadata.")
                val columnEntryRaw = columnMetadataStore.get(this.xodusTx, NameBinding.Column.toEntry(this@DefaultColumn.name))
                    ?: throw DatabaseException.DataCorruptionException("Failed to load column metadata entry for column '${this@DefaultColumn.name}'.")
                val columnEntry = ColumnMetadata.fromEntry(columnEntryRaw)
                this.autoIncrementCurrent = columnEntry.currentAutoIncrement
            }
        }

        /**
         * Gets and returns [ValueStatistics] for this [ColumnTx]
         *
         * @return [ValueStatistics].
         */
        @Suppress("UNCHECKED_CAST")
        override fun statistics(): ValueStatistics<T> = this.txLatch.withLock {
            val statistics = this.transaction.manager.statistics[this@DefaultColumn.name]?.statistics
            if (statistics != null) {
                return statistics as ValueStatistics<T>
            }
            return this.columnDef.type.defaultStatistics()
        }

        /**
         * Returns next auto-increment value for this [Column].
         *
         * Only supported for [Types.Int], [Types.Long] and [Types.Uuid] and only if the [ColumnDef] has auto-increment enabled.
         *
         * @return Next auto-increment value.
         */
        @Suppress("UNCHECKED_CAST")
        override fun nextAutoValue(): T = this.txLatch.withLock {
            check(this@DefaultColumn.columnDef.autoIncrement) { "Auto-value is only provided for columns with auto-increment." }
            val ret =  when (this.columnDef.type) {
                Types.Int -> IntValue(this.autoIncrementCurrent++)
                Types.Long -> LongValue(this.autoIncrementCurrent++)
                Types.Uuid -> UuidValue(UUID.randomUUID())
                else -> throw IllegalStateException("Auto-value is only provided for columns that support auto-increment.")
            } as T
            this.autoIncrementDirty = true
            ret
        }

        /**
         * Truncates (i.e., clears) the [DefaultEntity] backed by this [Name.EntityName].
         */
        override fun truncate() = this.txLatch.withLock {
            this.xodusTx.environment.truncateStore(this@DefaultColumn.name.storeName(), this.xodusTx)
            this.store = this.xodusTx.environment.openStore(this@DefaultColumn.name.storeName(), StoreConfig.WITHOUT_DUPLICATES, this.xodusTx)
        }

        /**
         * Drops the [DefaultEntity] backed by this [Name.EntityName].
         */
        override fun drop() = this.txLatch.withLock {
            /* Remove the data store. */
            this.xodusTx.environment.removeStore(this@DefaultColumn.name.storeName(), this.xodusTx)

            /* Now remove all catalogue entries related to column. */
            val columnMetadataStore = this.xodusTx.environment.openStore(DefaultCatalogue.COLUMN_METADATA_STORE_NAME, StoreConfig.USE_EXISTING, this.xodusTx)
            if (!columnMetadataStore.delete(this.xodusTx, NameBinding.Column.toEntry(this@DefaultColumn.name))) {
                throw DatabaseException.DataCorruptionException("DROP COLUMN $name failed: Failed to delete catalogue entry.")
            }
        }

        /**
         * Flushes in-memory representation of auto-increment counter to disk, if needed.
         */
        override fun beforeCommit() = this.txLatch.withLock {
            if (this.autoIncrementDirty) {
                val columnMetadataStore = this.xodusTx.environment.openStore(DefaultCatalogue.COLUMN_METADATA_STORE_NAME, StoreConfig.USE_EXISTING, this.xodusTx, false)
                    ?: throw DatabaseException.DataCorruptionException("Failed to open store for column metadata.")
                val columnEntryRaw = columnMetadataStore.get(this.xodusTx, NameBinding.Column.toEntry(this@DefaultColumn.name))
                    ?: throw DatabaseException.DataCorruptionException("Failed to load column metadata entry for column '${this@DefaultColumn.name}'.")
                val columnEntry = ColumnMetadata.fromEntry(columnEntryRaw).copy(currentAutoIncrement = this.autoIncrementCurrent)
                this.store.put(this.xodusTx, NameBinding.Column.toEntry(this@DefaultColumn.name), ColumnMetadata.toEntry(columnEntry))
                this.autoIncrementDirty = false
            }
        }
    }
}