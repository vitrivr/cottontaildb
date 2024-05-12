package org.vitrivr.cottontail.dbms.column

import it.unimi.dsi.fastutil.longs.Long2ObjectFunction
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
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
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.toKey
import org.vitrivr.cottontail.dbms.column.ColumnMetadata.Companion.storeName
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.SubTransaction
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.schema.DefaultSchema
import org.vitrivr.cottontail.dbms.statistics.defaultStatistics
import org.vitrivr.cottontail.dbms.statistics.values.ValueStatistics
import org.vitrivr.cottontail.storage.blob.VectorFile
import org.vitrivr.cottontail.storage.blob.VectorId
import org.vitrivr.cottontail.storage.serializers.tablets.Compression
import java.nio.file.Paths

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
class FixedLengthColumn<T : Value>(override val columnDef: ColumnDef<T>, override val parent: DefaultEntity) : Column<T> {

    /** The [Name.ColumnName] of this [VariableLengthColumn]. */
    override val name: Name.ColumnName
        get() = this.columnDef.name

    /** A [VariableLengthColumn] belongs to the same [DefaultCatalogue] as the [DefaultEntity] it belongs to. */
    override val catalogue: DefaultCatalogue
        get() = this.parent.catalogue

    /** An internal cache of all ongoing [DefaultSchema.Tx]s for this [DefaultSchema]. */
    private val transactions = Long2ObjectOpenHashMap<FixedLengthColumn<T>.Tx>()

    init {
        require(type !is Types.String && this.columnDef.type !is Types.ByteString) { "FixedLengthColumn can only be used for fixed-length types." }
    }

    /**
     * Creates and returns a new [VariableLengthColumn.Tx] for the given [EntityTx].
     *
     * @param parent The parent [EntityTx] to create the [VariableLengthColumn.Tx] for.
     * @return New [VariableLengthColumn.Tx]
     */
    override fun newTx(parent: EntityTx): ColumnTx<T> {
        require(parent is DefaultEntity.Tx) { "FixedLengthColumn can only be accessed with a DefaultEntity.Tx!" }
        return this.transactions.computeIfAbsent(parent.context.txn.transactionId, Long2ObjectFunction {
            val subTransaction = Tx(parent)
            parent.context.txn.registerSubtransaction(subTransaction)
            subTransaction
        })
    }

    /**
     * A [Tx] that affects this [VariableLengthColumn].
     */
    inner class Tx(override val parent: DefaultEntity.Tx): ColumnTx<T>, SubTransaction.WithCommit {

        /** Reference to the surrounding entities Xodus transaction. */
        internal val xodusTx = this.parent.xodusTx

        /** The [VectorFile] backing this [FixedLengthColumn]. */
        internal val file = VectorFile(Paths.get(this.xodusTx.environment.location).resolve(this.dbo.name.simple), this@FixedLengthColumn.type)

        /** Internal data [Store] reference. */
        private val store: Store = this.xodusTx.environment.openStore(
            this@FixedLengthColumn.name.storeName(),
            StoreConfig.WITHOUT_DUPLICATES,
            this.xodusTx
        )

        /** Reference to [Column] this [Tx] belongs to. */
        override val dbo: FixedLengthColumn<T>
            get() = this@FixedLengthColumn

        /**
         * Gets and returns [ValueStatistics] for this [ColumnTx]
         *
         * @return [ValueStatistics].
         */
        @Suppress("UNCHECKED_CAST")
        @Synchronized
        override fun statistics(): ValueStatistics<T> {
            val statistics = this.context.statistics[this@FixedLengthColumn.name]?.statistics as? ValueStatistics<T>
            if (statistics != null) return statistics
            return this.columnDef.type.defaultStatistics()
        }

        /**
         * Reads an entry from this [Column].
         *
         * @param tupleId The ID of the desired entry
         * @return The desired [Value] or null.
         */
        @Synchronized
        override fun read(tupleId: TupleId): T? {
            val vectorIdRaw = this.store.get(this.xodusTx, tupleId.toKey()) ?: return null
            val vectorId: VectorId = LongBinding.compressedEntryToLong(vectorIdRaw)
            return this.file.get(vectorId)
        }

        /**
         * Writes an entry to this [Column].
         *
         * @param tupleId The ID of the desired entry
         * @param value The new value [T] for the entry (or null).
         * @return The previous [Value] entry.
         */
        @Synchronized
        override fun write(tupleId: TupleId, value: T): T? {
            val old = this.read(tupleId)
            val vectorId = this.file.append(value)
            this.store.put(this.xodusTx, tupleId.toKey(), vectorId.toKey())
            return old
        }

        /**
         * Deletes an entry from this [Column].
         *
         * @param tupleId The ID of the entry to delete.
         * @return The previous [Value] entry.
         */
        override fun delete(tupleId: TupleId): T? {
            val old = this.read(tupleId)
            this.store.delete(this.xodusTx, tupleId.toKey())
            return old
        }

        /**
         * Returns a [Cursor] that can be used to iterate over all entries in this [Column].
         *
         * @return [Cursor]
         */
        @Suppress("UNCHECKED_CAST")
        @Synchronized
        override fun cursor(): Cursor<T?> {
            val count = this.store.count(this.xodusTx)
            if (count == 0L) return EmptyColumnCursor as Cursor<T?>
            return FixedLengthCursor(this)
        }

        /**
         *
         */
        override fun commit() {
            /* No op */
        }

        /**
         * Flushes the [VectorFile] backing this [FixedLengthColumn] to disk before committing.
         */
        override fun prepareCommit(): Boolean {
            this.file.flush()
            return true
        }
    }
}