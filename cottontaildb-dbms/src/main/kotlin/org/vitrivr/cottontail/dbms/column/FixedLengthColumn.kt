package org.vitrivr.cottontail.dbms.column

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.tablets.AbstractTablet
import org.vitrivr.cottontail.core.values.tablets.Tablet
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.general.AbstractTx
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.statistics.defaultStatistics
import org.vitrivr.cottontail.dbms.statistics.values.ValueStatistics
import org.vitrivr.cottontail.storage.serializers.SerializerFactory
import org.vitrivr.cottontail.storage.serializers.tablets.Compression
import org.vitrivr.cottontail.storage.serializers.tablets.TabletSerializer
import kotlin.concurrent.withLock

/**
 * The default [Column] implementation for fixed-length columns based on JetBrains Xodus. [Value]s are stored in [AbstractTablet]s of 64 entries.
 *
 * Only works for fixed-length types.
 *
 * @see Column
 * @see ColumnTx
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class FixedLengthColumn<T : Value>(override val columnDef: ColumnDef<T>, override val parent: DefaultEntity, val compression: Compression) : Column<T> {

    companion object {
        const val TABLET_SIZE = 128
        const val TABLET_SHR = 7
    }


    /** The [Name.ColumnName] of this [VariableLengthColumn]. */
    override val name: Name.ColumnName
        get() = this.columnDef.name

    /** A [VariableLengthColumn] belongs to the same [DefaultCatalogue] as the [DefaultEntity] it belongs to. */
    override val catalogue: DefaultCatalogue
        get() = this.parent.catalogue

    init {
        require(type !is Types.String && this.columnDef.type !is Types.ByteString) {
            "FixedLengthColumn can only be used for fixed-length types."
        }
    }

    /**
     * Creates and returns a new [VariableLengthColumn.Tx] for the given [QueryContext].
     *
     * @param context The [QueryContext] to create the [VariableLengthColumn.Tx] for.
     * @return New [VariableLengthColumn.Tx]
     */
    override fun newTx(context: QueryContext): ColumnTx<T> = context.txn.getCachedTxForDBO(this) ?: this.Tx(context)

    /**
     * A [Tx] that affects this [VariableLengthColumn].
     */
    inner class Tx constructor(context: QueryContext) : AbstractTx(context), ColumnTx<T>, org.vitrivr.cottontail.dbms.general.Tx.WithCommitFinalization {

        init {
            /* Cache this Tx for future use. */
            context.txn.cacheTx(this)
        }

        /** Internal data [Store] reference. */
        private val dataStore: Store = this@FixedLengthColumn.catalogue.transactionManager.environment.openStore(
            this@FixedLengthColumn.name.storeName(),
            StoreConfig.USE_EXISTING,
            this.context.txn.xodusTx,
            false
        ) ?: throw DatabaseException.DataCorruptionException("Data store for column ${this@FixedLengthColumn.name} is missing.")

        /** The internal [TabletSerializer] reference used for de-/serialization. */
        private val serializer: TabletSerializer<T> = SerializerFactory.tablet(this@FixedLengthColumn.columnDef.type, TABLET_SIZE, this@FixedLengthColumn.compression)

        /** The [TupleId] of the currently loaded [Tablet]. -1 if no [Tablet] has been loaded. */
        private var tabletId: TupleId = -1

        /** The currently loaded [Tablet]. */
        private var tablet: Tablet<T>? = null

        /** Flag indicating, that the currently loaded [Tablet] has been changed. */
        private var dirty: Boolean = false

        /** Reference to [Column] this [Tx] belongs to. */
        override val dbo: FixedLengthColumn<T>
            get() = this@FixedLengthColumn

        /**
         * Gets and returns [ValueStatistics] for this [ColumnTx]
         *
         * @return [ValueStatistics].
         */
        override fun statistics(): ValueStatistics<T> = this.txLatch.withLock {
            val statistics = this@FixedLengthColumn.catalogue.statisticsManager[this@FixedLengthColumn.name]?.statistics as? ValueStatistics<T>
            if (statistics != null) return statistics
            return this.columnDef.type.defaultStatistics()
        }

        /**
         * Reads an entry from this [Column].
         *
         * @param tupleId The ID of the desired entry
         * @return The desired [Value] or null.
         */
        override fun read(tupleId: TupleId): T? = this.txLatch.withLock {
            val tabletId = tupleId ushr TABLET_SHR
            val tabletIdx = (tupleId % TABLET_SIZE).toInt()
            loadTabletF(tabletId)
            return this.tablet!![tabletIdx]
        }

        /**
         * Writes an entry to this [Column].
         *
         * @param tupleId The ID of the desired entry
         * @param value The new value [T] for the entry (or null).
         * @return The desired entry.
         */
        override fun write(tupleId: TupleId, value: T) = this.txLatch.withLock {
            val tabletId = tupleId ushr TABLET_SHR
            val tabletIdx = (tupleId % TABLET_SIZE).toInt()
            loadTabletF(tabletId)
            val old = this.tablet!![tabletIdx]
            this.tablet!![tabletIdx] = value
            this.dirty = true
            old
        }

        /**
         * Deletes an entry from this [Column].
         *
         * @param tupleId The ID of the entry to delete.
         * @return The previous [Value] entry.
         */
        override fun delete(tupleId: TupleId): T? = this.txLatch.withLock {
            val tabletId = tupleId ushr TABLET_SHR
            val tabletIdx = (tupleId % TABLET_SIZE).toInt()
            loadTabletF(tabletId)
            val old = this.tablet!![tabletIdx]
            this.tablet!![tabletIdx] = null
            this.dirty = true
            old
        }

        /**
         * Returns a [Cursor] that can be used to iterate over all entries in this [Column].
         *
         * @return [Cursor]
         */
        @Suppress("UNCHECKED_CAST")
        override fun cursor(): Cursor<T> {
            val count = this.dataStore.count(this.context.txn.xodusTx)
            if (count == 0L) return EmptyColumnCursor as Cursor<T>
            return FixedLengthCursor(this)
        }

        /**
         * Flushes in-memory [AbstractTablet] to disk, if needed.
         */
        override fun beforeCommit() {
            this.flushTablet()
        }

        /**
         * Loads the [AbstractTablet] with the specified [TupleId] into memory.
         *
         * @param tabletId [TupleId] of the [AbstractTablet] to load.
         */
        private fun loadTabletF(tabletId: TupleId) {
            if (this.tabletId == tabletId) return
            this.flushTablet() /* Flush tablet to disk if necessary. */
            this.tabletId = tabletId
            val rawTablet = this.dataStore.get(this.context.txn.xodusTx, LongBinding.longToCompressedEntry(tabletId))
            if (rawTablet != null) {
                this.tablet = this.serializer.fromEntry(rawTablet)
            } else {
                this.tablet = Tablet.of(TABLET_SIZE, this@FixedLengthColumn.type)
            }
        }

        /**
         * Flushes the currently active tablet to disk, if necessary.
         */
        private fun flushTablet() {
            if (this.dirty && this.tablet != null) { /* Flush current tablet if needed. */
                this.dataStore.put(this.context.txn.xodusTx, LongBinding.longToCompressedEntry(this.tabletId), this.serializer.toEntry(this.tablet!!))
                this.dirty = false
            }
        }
    }
}