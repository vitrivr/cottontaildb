package org.vitrivr.cottontail.legacy.v2.column

import org.mapdb.CottontailStoreWAL
import org.mapdb.DBException
import org.mapdb.Serializer
import org.mapdb.Store
import org.vitrivr.cottontail.config.MapDBConfig
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.column.Column
import org.vitrivr.cottontail.dbms.column.ColumnEngine
import org.vitrivr.cottontail.dbms.column.ColumnTx
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.exceptions.TransactionException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.general.AbstractTx
import org.vitrivr.cottontail.dbms.general.DBOVersion
import org.vitrivr.cottontail.dbms.statistics.columns.ValueStatistics
import org.vitrivr.cottontail.legacy.v1.column.ColumnV1
import org.vitrivr.cottontail.storage.serializers.values.ValueSerializerFactory
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.locks.StampedLock

/**
 * Represents a [Column] in the Cottontail DB model that uses the Map DB storage engine.
 *
 * @see DefaultEntity
 *
 * @param <T> Type of the value held by this [ColumnV2].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class ColumnV2<T : Value>(val path: Path, override val parent: Entity) : Column<T>, AutoCloseable {
    /**
     * Companion object with some important constants.
     */
    companion object {
        /**
         * The shift between Cottontail DB's [TupleId] and the record ID used by MapDB.
         *
         * Cottontail DB's [TupleId]'s are zero based.
         * MapDB's record ID is 1 based + the first record is reserved for the header.
         */
        private const val RECORD_ID_TUPLE_ID_SHIFT = 2L


        /** Record ID of the [ColumnV2Header]. */
        private const val HEADER_RECORD_ID: Long = 1L

        /**
         * Initializes a new, empty [ColumnV2]
         *
         * @param path The [Path] to the folder in which the [ColumnV2] file should be stored.
         * @param column The [ColumnDef] that specifies the [ColumnV2]
         * @param config The [MapDBConfig] used to initialize the [ColumnV2]
         */
        fun initialize(path: Path, column: ColumnDef<*>, config: MapDBConfig) {
            if (Files.exists(path)) throw DatabaseException.InvalidFileException("Could not initialize column ${column.name}. A file already exists under $path.")
            val store = config.store(path)
            store.put(ColumnV2Header(column), ColumnV2Header)
            store.commit()
            store.close()
        }
    }

    /** Internal reference to the [Store] underpinning this [ColumnV2]. */
    private var store: CottontailStoreWAL = try {
        this.parent.parent.parent.config.mapdb.store(this.path)
    } catch (e: DBException) {
        throw DatabaseException("Failed to open column at '$path': ${e.message}'")
    }

    /** Internal reference to the header of this [ColumnV2]. */
    private val header
        get() = this.store.get(HEADER_RECORD_ID, ColumnV2Header)
            ?: throw DatabaseException.DataCorruptionException("Failed to open header of column '$name'!'")

    /** Getter for this [ColumnV2]'s [ColumnDef]. Can be stored since [ColumnV2]s [ColumnDef] is immutable. */
    @Suppress("UNCHECKED_CAST")
    override val columnDef: ColumnDef<T> = this.header.columnDef as ColumnDef<T>

    /** The [Name.ColumnName] of this [ColumnV2]. Can be stored since [ColumnV2]s [ColumnDef] is immutable. */
    override val name: Name.ColumnName
        get() = this.columnDef.name

    /** The [Catalogue] this [ColumnV2] belongs to. */
    override val catalogue: Catalogue
        get() = this.parent.catalogue

    /** The [DBOVersion] of this [ColumnV2]. */
    override val version: DBOVersion
        get() = DBOVersion.V2_0

    /** The [ColumnEngine] for [ColumnV2]. */
    val engine: ColumnEngine
        get() = ColumnEngine.MAPDB

    /** Status indicating whether this [ColumnV2] is open or closed. */
    override val closed: Boolean
        get() = this.store.isClosed

    /** An internal lock that is used to synchronize closing of the[ColumnV2] in presence of ongoing [ColumnV2.Tx]. */
    private val closeLock = StampedLock()

    /**
     * Creates and returns a new [ColumnV2.Tx] for the given [TransactionContext].
     *
     * @param context The [TransactionContext] to create the [ColumnV2.Tx] for.
     * @return New [ColumnV2.Tx]
     */
    override fun newTx(context: TransactionContext) = Tx(context)

    /**
     * Closes the [ColumnV2]. Closing an [ColumnV2] is a delicate matter since ongoing [ColumnV2.Tx]  are involved.
     * Therefore, access to the method is mediated by an global [ColumnV2] wide lock.
     */
    override fun close() {
        if (!this.closed) {
            this.store.close()
        }
    }

    /**
     * A [Tx] that affects this [ColumnV2].
     *
     * @author Ralph Gasser
     * @version 1.4.0
     */
    inner class Tx constructor(context: TransactionContext) : AbstractTx(context), ColumnTx<T> {

        /** Reference to the [ColumnV2] this [ColumnV2.Tx] belongs to. */
        override val dbo: Column<T>
            get() = this@ColumnV2

        /** The [Serializer] used for de-/serialization of [ColumnV2] entries. */
        private val serializer = ValueSerializerFactory.mapdb(this@ColumnV2.type)

        /** Obtains a global (non-exclusive) read-lock on [ColumnV2]. Prevents enclosing [ColumnV2] from being closed while this [ColumnV2.Tx] is still in use. */
        private val closeStamp = this@ColumnV2.closeLock.readLock()

        /** Tries to acquire a global read-lock on the surrounding column. */
        init {
            if (this@ColumnV2.closed) {
                this@ColumnV2.closeLock.unlockRead(this.closeStamp)
                throw TransactionException.DBOClosed(this.context.txId, this@ColumnV2)
            }
        }

        /**
         * Gets and returns an entry from this [ColumnV2].
         *
         * @param tupleId The ID of the desired entry
         * @return The desired entry.
         *
         * @throws DatabaseException If the tuple with the desired ID doesn't exist OR is invalid.
         */
        override fun get(tupleId: Long): T? {
            return this@ColumnV2.store.get(tupleId + RECORD_ID_TUPLE_ID_SHIFT, this.serializer)
        }

        /**
         * Returns the number of entries in this [ColumnV2]. Action acquires a global read dataLock for the [ColumnV2].
         *
         * @return The number of entries in this [ColumnV2].
         */
        override fun count(): Long {
            return this@ColumnV2.header.count
        }

        /**
         * The smallest [TupleId] contained in this [ColumnV1].
         *
         * @return [TupleId]
         */
        override fun smallestTupleId(): TupleId = 1L

        /**
         * The largest [TupleId] contained in this [ColumnV1].
         *
         * @return [TupleId]
         */
        override fun largestTupleId(): TupleId = this@ColumnV2.store.maxRecid

        /**
         * Creates and returns a new [Iterator] for this [ColumnV2.Tx] that returns
         * all [TupleId]s contained within the surrounding [ColumnV2].
         *
         * @return [Iterator]
         */
        fun scan() = this.scan(this.smallestTupleId()..this.largestTupleId())

        /**
         * Creates and returns a new [Iterator] for this [ColumnV2.Tx] that returns
         * all [TupleId]s contained within the surrounding [ColumnV2] and a certain range.
         *
         * @param range The [LongRange] that should be scanned.
         * @return [Iterator]
         */
        fun scan(range: LongRange) = object : Iterator<TupleId> {
            private val wrapped = this@ColumnV2.store.RecordIdIterator((range.first + RECORD_ID_TUPLE_ID_SHIFT)..(range.last + RECORD_ID_TUPLE_ID_SHIFT))
            override fun hasNext(): Boolean = this.wrapped.hasNext()
            override fun next(): TupleId = this.wrapped.next() - RECORD_ID_TUPLE_ID_SHIFT
        }

        override fun contains(tupleId: TupleId): Boolean {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }

        override fun add(tupleId: TupleId, value: T?): Boolean {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }

        override fun update(tupleId: Long, value: T?): T? {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }

        override fun delete(tupleId: Long): T? {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }

        override fun statistics(): ValueStatistics<T> {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }

        /**
         * Releases the [closeLock] on the [ColumnV2].
         */
        override fun cleanup() {
            this@ColumnV2.closeLock.unlockRead(this.closeStamp)
        }

        override fun cursor(): Cursor<T?> {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }

        override fun cursor(partition: LongRange): Cursor<T?> {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }

        override fun clear() {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }
    }
}