package org.vitrivr.cottontail.dbms.column

import jetbrains.exodus.bindings.LongBinding
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.Value
import org.vitrivr.cottontail.core.values.tablets.Tablet
import org.vitrivr.cottontail.core.values.tablets.bytebuffer.AbstractByteBufferTablet
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.storage.serializers.SerializerFactory
import org.vitrivr.cottontail.storage.serializers.tablets.Compression
import org.vitrivr.cottontail.storage.serializers.tablets.TabletSerializer
import kotlin.concurrent.withLock

typealias TabletId = Long

/**
 * The default [Column] implementation for fixed-length columns based on JetBrains Xodus.
 *
 * [Value]s are stored in [AbstractByteBufferTablet]s. Only works for fixed-length types.
 *
 * @see Column
 * @see ColumnTx
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class FixedLengthColumn<T : Value>(columnDef: ColumnDef<T>, parent: DefaultEntity, private val compression: Compression) : DefaultColumn<T>(columnDef, parent) {

    /** The [Name.ColumnName] of this [VariableLengthColumn]. */
    override val name: Name.ColumnName
        get() = this.columnDef.name

    /** A [VariableLengthColumn] belongs to the same [DefaultCatalogue] as the [DefaultEntity] it belongs to. */
    override val catalogue: DefaultCatalogue
        get() = this.parent.catalogue

    /** The size (i.e., the number of entries) per [AbstractByteBufferTablet]. */
    private val tabletSize: Int = 128

    init {
        require(type !is Types.String && this.columnDef.type !is Types.ByteString) { "FixedLengthColumn can only be used for fixed-length types." }
    }

    /**
     * Creates and returns a new [VariableLengthColumn.Tx] for the given [QueryContext].
     *
     * @param parent The [EntityTx] to create the [VariableLengthColumn.Tx] for.
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

        /** The internal [TabletSerializer] reference used for de-/serialization. */
        private val serializer: TabletSerializer<T> = SerializerFactory.tablet(this@FixedLengthColumn.columnDef.type, this@FixedLengthColumn.tabletSize)

        /** The [TabletId] of the currently loaded [Tablet]. -1 if no [Tablet] has been loaded. */
        private var tabletId: TabletId = -1

        /** The currently loaded [Tablet]. */
        private var tablet: AbstractByteBufferTablet<T>? = null

        /** The flag indicating, that there is a dirty in-memory tablet. */
        @Volatile
        private var tabletDirty = false

        /** Reference to [Column] this [Tx] belongs to. */
        override val dbo: FixedLengthColumn<T>
            get() = this@FixedLengthColumn

        /**
         * Reads an entry from this [Column].
         *
         * @param tupleId The ID of the desired entry
         * @return The desired [Value] or null.
         */
        override fun read(tupleId: TupleId): T? = this.txLatch.withLock {
            val tabletId = tupleId / this@FixedLengthColumn.tabletSize
            val position = (tupleId % Long.SIZE_BITS).toInt()
            if (this.tabletId != tabletId) {
                loadTablet(tabletId)
            }
            return this.tablet!![position]
        }

        /**
         * Writes an entry to this [Column].
         *
         * @param tupleId The ID of the desired entry
         * @param value The new value [T] for the entry (or null).
         * @return The desired entry.
         */
        override fun write(tupleId: TupleId, value: T) = this.txLatch.withLock {
            val tabletId = tupleId / this@FixedLengthColumn.tabletSize
            val position = (tupleId % Long.SIZE_BITS).toInt()
            if (this.tabletId != tabletId) {
                loadTablet(tabletId)
            }
            val old = this.tablet!![position]
            this.tablet!![position] = value
            this.tabletDirty = true
            old
        }

        /**
         * Deletes an entry from this [Column].
         *
         * @param tupleId The ID of the entry to delete.
         * @return The previous [Value] entry.
         */
        override fun delete(tupleId: TupleId): T? = this.txLatch.withLock {
            val tabletId = tupleId ushr 6
            val position = (tupleId % tabletId).toInt()
            if (this.tabletId != tabletId) {
                loadTablet(tabletId)
            }
            val old = this.tablet!![position]
            this.tablet!![position] = null
            this.tabletDirty = true
            old
        }

        /**
         * Returns a [Cursor] that can be used to iterate over all entries in this [Column].
         *
         * @return [Cursor]
         */
        @Suppress("UNCHECKED_CAST")
        override fun cursor(): Cursor<T> {
            if (this.store.count(this.xodusTx) == 0L) return EmptyColumnCursor as Cursor<T>
            return FixedLengthCursor(this)
        }

        /**
         * Flushes in-memory [AbstractByteBufferTablet] to disk, if needed.
         */
        override fun beforeCommit() {
            super.beforeCommit()
            val tablet = this.tablet
            if ( this.tabletDirty && tablet != null) {
                this.store.put(this.xodusTx, LongBinding.longToCompressedEntry(this.tabletId), this.serializer.toEntry(tablet))
                this.tabletDirty = false
            }
        }

        /**
         * Loads the [AbstractByteBufferTablet] with the specified [TabletId] into memory.
         *
         * @param tabletId [TabletId] of the [AbstractByteBufferTablet] to load.
         */
        private fun loadTablet(tabletId: TabletId) {
            if (this.tabletDirty && this.tablet != null) { /* Flush current tablet if needed. */
                this.store.put(this.xodusTx, LongBinding.longToCompressedEntry(this.tabletId), this.serializer.toEntry(this.tablet!!))
                this.tabletDirty = false
            }
            this.tabletId = tabletId
            val rawTablet = this.store.get(this.xodusTx, LongBinding.longToCompressedEntry(tabletId))
            if (rawTablet != null) {
                this.tablet = this.serializer.fromEntry(rawTablet)
            } else {
                this.tablet = AbstractByteBufferTablet.of(this@FixedLengthColumn.tabletSize, this@FixedLengthColumn.type)
            }
        }
    }
}