package org.vitrivr.cottontail.legacy.v2.entity

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import org.mapdb.DB
import org.mapdb.DBException
import org.mapdb.StoreWAL
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.column.Column
import org.vitrivr.cottontail.dbms.column.ColumnTx
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.exceptions.TransactionException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.general.AbstractTx
import org.vitrivr.cottontail.dbms.general.DBOVersion
import org.vitrivr.cottontail.dbms.index.Index
import org.vitrivr.cottontail.dbms.index.IndexConfig
import org.vitrivr.cottontail.dbms.index.IndexTx
import org.vitrivr.cottontail.dbms.index.IndexType
import org.vitrivr.cottontail.dbms.schema.DefaultSchema
import org.vitrivr.cottontail.dbms.statistics.entity.EntityStatistics
import org.vitrivr.cottontail.legacy.v2.column.ColumnV2
import org.vitrivr.cottontail.legacy.v2.schema.SchemaV2
import org.vitrivr.cottontail.utilities.extensions.write
import java.nio.file.Path
import java.util.concurrent.locks.StampedLock

/**
 * Represents a single entity in the Cottontail DB data model. An [EntityV2] has name that must remain unique within a [DefaultSchema].
 * The [EntityV2] contains one to many [Column]s holding the actual data. Hence, it can be seen as a table containing tuples.
 *
 * Calling the default constructor for [EntityV2] opens that [EntityV2]. It can only be opened once due to file locks and it
 * will remain open until the [Entity.close()] method is called.
 *
 * @see DefaultSchema
 * @see Column
 * @see EntityTx
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class EntityV2(val path: Path, override val parent: SchemaV2) : Entity, AutoCloseable {
    /**
     * Companion object of the [EntityV2]
     */
    companion object {
        /** Filename for the [EntityV2] catalogue.  */
        const val CATALOGUE_FILE = "index.db"

        /** Field name for the [EntityV2] header field.  */
        const val ENTITY_HEADER_FIELD = "cdb_entity_header"

        /** Field name for the [EntityV2]'s statistics.  */
        const val ENTITY_STATISTICS_FIELD = "cdb_entity_statistics"
    }

    /** Internal reference to the [StoreWAL] underpinning this [EntityV2]. */
    private val store: DB = try {
        this.parent.parent.config.mapdb.db(this.path.resolve(CATALOGUE_FILE))
    } catch (e: DBException) {
        throw DatabaseException("Failed to open entity at $path: ${e.message}'.")
    }

    /** The [EntityHeader] of this [EntityV2]. */
    private val headerField = this.store.atomicVar(ENTITY_HEADER_FIELD, EntityHeader.Serializer).createOrOpen()

    /** The maximum [TupleId] in this [EntityV2]. */
    private val statisticsField = this.store.atomicVar(ENTITY_STATISTICS_FIELD, EntityStatistics.Serializer).createOrOpen()

    /** The [Name.EntityName] of this [EntityV2]. */
    override val name: Name.EntityName = this.parent.name.entity(this.headerField.get().name)

    /** An internal lock that is used to synchronize access to this [EntityV2] in presence of ongoing [Tx]. */
    private val closeLock = StampedLock()

    /** List of all the [Column]s associated with this [EntityV2]; Iteration order of entries as defined in schema! */
    private val columns: MutableMap<Name.ColumnName, ColumnV2<*>> = Object2ObjectLinkedOpenHashMap()

    /** Local snapshot of the surrounding [Entity]'s [Index]es. */
    private val indexes: MutableMap<Name.IndexName, BrokenIndexV2> = Object2ObjectLinkedOpenHashMap()

    /** The [Catalogue] this [EntityV2] belongs to. */
    override val catalogue: Catalogue
        get() = this.parent.catalogue

    /** The [DBOVersion] of this [EntityV2]. */
    override val version: DBOVersion
        get() = DBOVersion.V2_0

    /** Status indicating whether this [EntityV2] is open or closed. */
    override val closed: Boolean
        get() = this.store.isClosed()

    init {
        /** Load and initialize the columns. */
        val header = this.headerField.get()
        header.columns.map {
            val columnName = this.name.column(it.name)
            val path = this.path.resolve("${it.name}.col")
            this.columns[columnName] = ColumnV2<Value>(path, this)
        }

        /** Load and initialize the indexes. */
        header.indexes.forEach {
            val indexName = this.name.index(it.name)
            val path = this.path.resolve("${it.name}.idx")
            this.indexes[indexName] = BrokenIndexV2(indexName, this, path)
        }
    }

    /**
     * Creates and returns a new [EntityV2.Tx] for the given [TransactionContext].
     *
     * @param context The [TransactionContext] to create the [EntityV2.Tx] for.
     * @return New [EntityV2.Tx]
     */
    override fun newTx(context: TransactionContext) = this.Tx(context)

    /**
     * Closes the [EntityV2].
     *
     * Closing an [EntityV2] is a delicate matter since ongoing [EntityV2.Tx] objects as well
     * as all involved [Column]s are involved.Therefore, access to the method is mediated by an
     * global [EntityV2] wide lock.
     */
    override fun close() = this.closeLock.write {
        if (!this.closed) {
            this.store.close()
            this.columns.values.forEach { it.close() }
        }
    }

    /**
     * A [Tx] that affects this [EntityV2]. Opening a [EntityV2.Tx] will automatically spawn [ColumnTx]
     * and [IndexTx] for every [Column] and [IndexTx] associated with this [EntityV2].
     *
     * @author Ralph Gasser
     * @version 1.3.0
     */
    inner class Tx(context: TransactionContext) : AbstractTx(context), EntityTx {

        /** Obtains a global non-exclusive lock on [EntityV2]. Prevents [EntityV2] from being closed. */
        private val closeStamp = this@EntityV2.closeLock.readLock()

        /** Reference to the surrounding [EntityV2]. */
        override val dbo: EntityV2
            get() = this@EntityV2



        /** Checks if DBO is still open. */
        init {
            if (this@EntityV2.closed) {
                this@EntityV2.closeLock.unlockRead(this.closeStamp)
                throw TransactionException.DBOClosed(this.context.txId, this@EntityV2)
            }
        }

        /**
         * Reads the values of one or many [Column]s and returns it as a [Record]
         *
         * @param tupleId The [TupleId] of the desired entry.
         * @param columns The [ColumnDef]s that should be read.
         * @return The desired [Record].
         *
         * @throws DatabaseException If tuple with the desired ID doesn't exist OR is invalid.
         */
        override fun read(tupleId: TupleId, columns: Array<ColumnDef<*>>): Record {
            /* Read values from underlying columns. */
            val values = columns.map {
                val column = this@EntityV2.columns[it.name] ?: throw IllegalArgumentException("Column $it does not exist on entity ${this@EntityV2.name}.")
                (this.context.getTx(column) as ColumnTx<*>).get(tupleId)
            }.toTypedArray()

            /* Return value of all the desired columns. */
            return StandaloneRecord(tupleId, columns, values)
        }

        /**
         * Returns the number of entries in this [EntityV2].
         *
         * @return The number of entries in this [EntityV2].
         */
        override fun count(): Long {
            return this@EntityV2.statisticsField.get().count
        }

        /**
         * Returns the minimum [TupleId] occupied by entries in this [EntityV2].
         *
         * @return The minimum [TupleId] occupied by entries in this [EntityV2].
         */
        override fun smallestTupleId(): TupleId = 0L

        /**
         * Returns the maximum [TupleId] occupied by entries in this [EntityV2].
         *
         * @return The maximum [TupleId] occupied by entries in this [EntityV2].
         */
        override fun largestTupleId(): TupleId {
            return this@EntityV2.statisticsField.get().maximumTupleId
        }

        /**
         * Lists all [Column]s for the [EntityV2] associated with this [EntityTx].
         *
         * @return List of all [Column]s.
         */
        override fun listColumns(): List<ColumnDef<*>> {
            return this@EntityV2.columns.values.map { it.columnDef }.toList()
        }

        /**
         * Returns the [ColumnDef] for the specified [Name.ColumnName].
         *
         * @param name The [Name.ColumnName] of the [Column].
         * @return [ColumnDef] of the [Column].
         */
        override fun columnForName(name: Name.ColumnName): Column<*> {
            return this@EntityV2.columns[name] ?: throw DatabaseException.ColumnDoesNotExistException(name)
        }

        /**
         * Lists all [Index] implementations that belong to this [EntityTx].
         *
         * @return List of [Name.IndexName] managed by this [EntityTx]
         */
        override fun listIndexes(): List<Name.IndexName> {
            return this@EntityV2.indexes.values.map { it.name }.toList()
        }

        /**
         * Lists [Name.IndexName] for all [Index] implementations that belong to this [EntityTx].
         *
         * @return List of [Name.IndexName] managed by this [EntityTx]
         */
        override fun indexForName(name: Name.IndexName): Index {
            return this@EntityV2.indexes[name] ?: throw DatabaseException.IndexDoesNotExistException(name)
        }

        override fun contains(tupleId: TupleId): Boolean {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }

        override fun createIndex(name: Name.IndexName, type: IndexType, columns: List<Name.ColumnName>, configuration: IndexConfig<*>): Index {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }

        override fun dropIndex(name: Name.IndexName) {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }

        override fun optimize() {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }

        /**
         * Creates and returns a new [Iterator] for this [EntityV2.Tx] that returns
         * all [TupleId]s contained within the surrounding [EntityV2].
         *
         * <strong>Important:</strong> It remains to the caller to close the [Iterator]
         *
         * @param columns The [ColumnDef]s that should be scanned.
         *
         * @return [Iterator]
         */
        override fun cursor(columns: Array<ColumnDef<*>>): Cursor<Record> = cursor(columns, this.smallestTupleId() .. this.largestTupleId())

        /**
         * Creates and returns a new [Iterator] for this [EntityV2.Tx] that returns all [TupleId]s
         * contained within the surrounding [EntityV2] and a certain range.
         *
         * @param columns The [ColumnDef]s that should be scanned.
         * @param partition The [LongRange] that describes the partition to open a [Cursor] for.
         *
         * @return [Iterator]
         */
        override fun cursor(columns: Array<ColumnDef<*>>, partition: LongRange) = object : Cursor<Record> {

            /** List of [ColumnTx]s used by  this [Iterator]. */
            private val txs = columns.map {
                val column = this@EntityV2.columns[it.name] ?: throw IllegalArgumentException("Column $it does not exist on entity ${this@EntityV2.name}.")
                (this@Tx.context.getTx(column) as ColumnV2<*>.Tx)
            }

            /** The wrapped [Iterator] of the first column. */
            private val wrapped = this.txs.first().scan(partition)

            /** Array of [Value]s emitted by this [EntityV2]. */
            private val values = arrayOfNulls<Value?>(columns.size)

            override fun value(): Record {
                val tupleId = this.wrapped.next()
                for ((i, tx) in this.txs.withIndex()) {
                    this.values[i] = tx.get(tupleId)
                }
                return StandaloneRecord(tupleId, columns, this.values)
            }
            override fun key(): TupleId = this.wrapped.next()
            override fun moveNext(): Boolean {
                return this.wrapped.hasNext()
            }
            override fun close() { /* No op. */ }
        }

        override fun insert(record: Record): TupleId {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }

        override fun update(record: Record) {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }

        override fun delete(tupleId: TupleId) {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }

        /**
         * Releases the [closeLock] on the [EntityV2].
         */
        override fun cleanup() {
            this@EntityV2.closeLock.unlockRead(this.closeStamp)
        }
    }
}