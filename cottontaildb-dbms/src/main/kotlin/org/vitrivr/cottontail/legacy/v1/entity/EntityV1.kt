package org.vitrivr.cottontail.legacy.v1.entity

import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import org.mapdb.CottontailStoreWAL
import org.mapdb.DBException
import org.mapdb.Serializer
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
import org.vitrivr.cottontail.dbms.general.AbstractTx
import org.vitrivr.cottontail.dbms.general.DBOVersion
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.index.basic.IndexConfig
import org.vitrivr.cottontail.dbms.index.basic.IndexType
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.legacy.v1.column.ColumnV1
import org.vitrivr.cottontail.legacy.v1.schema.SchemaV1
import java.io.Closeable
import java.nio.file.Path
import kotlin.concurrent.withLock

/**
 * Represents a single entity in the Cottontail DB data model. An [Entity] has name that must remain
 * unique within a [SchemaV1]. The [Entity] contains one to many [Column]s holding the actual data.
 * Hence, it can be seen as a table containing tuples.
 *
 * Calling the default constructor for [Entity] opens that [Entity]. It can only be opened once due
 * to file locks and it will remain open until the [Entity.close()] method is called.
 *
 * @see SchemaV1
 * @see Column
 * @see EntityTx
 *
 * @author Ralph Gasser
 * @version 2.0.1
 */
class EntityV1(override val name: Name.EntityName, override val parent: SchemaV1) : Entity, Closeable {
    /**
     * Companion object of the [Entity]
     */
    companion object {
        /** Filename for the [Entity] catalogue.  */
        const val FILE_CATALOGUE = "index.db"

        /** Filename for the [Entity] catalogue.  */
        const val HEADER_RECORD_ID = 1L
    }

    /** The [Path] to the [Entity]'s main folder. */
    val path: Path = this.parent.path.resolve("entity_${name.simple}")

    /** Internal reference to the [StoreWAL] underpinning this [Entity]. */
    private val store: CottontailStoreWAL = try {
        this.parent.parent.config.mapdb.store(this.path.resolve(FILE_CATALOGUE))
    } catch (e: DBException) {
        throw DatabaseException("Failed to open entity '$name': ${e.message}'.")
    }

    /** The header of this [Entity]. */
    private val header: EntityV1Header
        get() = this.store.get(HEADER_RECORD_ID, EntityV1Header.Serializer)
            ?: throw DatabaseException.DataCorruptionException("Failed to open header of entity '$name'!")

    /** List of all the [Column]s associated with this [Entity]; Iteration order of entries as defined in schema! */
    private val columns: MutableMap<Name.ColumnName, ColumnV1<*>> = Object2ObjectLinkedOpenHashMap()

    /** List of all the [Index]es associated with this [Entity]. */
    private val indexes: MutableMap<Name.IndexName, Index> = Object2ObjectOpenHashMap()

    init {
        /* Initialize columns. */
        this.header.columns.map {
            val columnName = this.name.column(this.store.get(it, Serializer.STRING) ?: throw DatabaseException.DataCorruptionException("Failed to open entity '$name': Could not read column definition at position $it!"))
            this.columns[columnName] = ColumnV1<Value>(columnName, this)
        }

        /* Initialize indexes (broken). */
        this.header.indexes.forEach { idx ->
            val indexEntry = this.store.get(idx, IndexV1Entry.Serializer) ?: throw DatabaseException.DataCorruptionException("Failed to open entity '$name': Could not read index definition at position $idx!")
            val indexName = this.name.index(indexEntry.name)
            this.indexes[indexName] = BrokenIndexV1(this.name.index(indexEntry.name), this, this.path.resolve(indexEntry.name), indexEntry.type)
        }
    }

    /** The [Catalogue] this [EntityV1] belongs to. */
    override val catalogue: Catalogue
        get() = this.parent.catalogue

    /** The [DBOVersion] of this [EntityV1]. */
    override val version: DBOVersion
        get() = DBOVersion.V1_0

    /**
     * Status indicating whether this [Entity] is open or closed.
     */
    @Volatile
    var closed: Boolean = false
        private set

    /**
     * Creates and returns a new [EntityTx] for the given [QueryContext].
     *
     * @param context The [QueryContext] to create the [EntityTx] for.
     * @return New [EntityTx]
     */
    override fun newTx(context: QueryContext) = this.Tx(context)

    /**
     * Closes the [Entity]. Closing an [Entity] is a delicate matter since ongoing [EntityTx] objects as well as all involved [Column]s are involved.
     * Therefore, access to the method is mediated by an global [Entity] wide lock.
     */
    override fun close() {
        if (!this.closed) {
            this.columns.values.forEach { it.close() }
            this.store.close()
            this.closed = true
        }
    }

    /**
     * A [Tx] that affects this [Entity].
     *
     * Opening a [EntityTx] will automatically spawn [ColumnTx] for every [Column] that belongs to this [Entity].
     */
    inner class Tx(context: QueryContext) : AbstractTx(context), EntityTx {

        /** Reference to the surrounding [Entity]. */
        override val dbo: Entity
            get() = this@EntityV1

        /** Tries to acquire a global read-lock on this entity. */
        init {
            if (this@EntityV1.closed) throw TransactionException.DBOClosed(this.context.txn.txId, this@EntityV1)
        }

        /**
         * Lists all [Column]s for the [Entity] associated with this [EntityTx].
         *
         * @return List of all [Column]s.
         */
        override fun listColumns(): List<ColumnDef<*>> {
            return this@EntityV1.columns.values.map { it.columnDef }.toList()
        }

        /**
         * Returns the [ColumnDef] for the specified [Name.ColumnName].
         *
         * @param name The [Name.ColumnName] of the [Column].
         * @return [ColumnDef] of the [Column].
         */
        override fun columnForName(name: Name.ColumnName): Column<*> = this.txLatch.withLock {
            if (!name.wildcard) {
                this@EntityV1.columns[name] ?: throw DatabaseException.ColumnDoesNotExistException(
                    name
                )
            } else {
                val fqn = this@EntityV1.name.column(name.simple)
                this@EntityV1.columns[fqn] ?: throw DatabaseException.ColumnDoesNotExistException(
                    fqn
                )
            }
        }

        /**
         * Lists all [Index] implementations that belong to this [EntityTx].
         *
         * @return List of [Name.IndexName] managed by this [EntityTx]
         */
        override fun listIndexes(): List<Name.IndexName> {
            return this@EntityV1.indexes.keys.toList()
        }

        /**
         * Lists [Name.IndexName] for all [Index] implementations that belong to this [EntityTx].
         *
         * @return List of [Name.IndexName] managed by this [EntityTx]
         */
        override fun indexForName(name: Name.IndexName): Index {
            return this@EntityV1.indexes[name] ?: throw DatabaseException.IndexDoesNotExistException(name)
        }

        override fun smallestTupleId(): TupleId {
            val columnTx = this@EntityV1.columns.values.first().newTx(this.context)
            return columnTx.smallestTupleId()
        }

        override fun largestTupleId(): TupleId {
            val columnTx = this@EntityV1.columns.values.first().newTx(this.context)
            return columnTx.largestTupleId()
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

        override fun read(tupleId: TupleId, columns: Array<ColumnDef<*>>): Record {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }

        override fun cursor(columns: Array<ColumnDef<*>>): Cursor<Record> = cursor(columns, this.smallestTupleId() .. this.largestTupleId())

        override fun cursor(columns: Array<ColumnDef<*>>, partition: LongRange): Cursor<Record> = object : Cursor<Record> {

            /** The wrapped [Iterator] of the first (primary) column. */
            private val wrapped = this@EntityV1.columns.values.first().newTx(this@Tx.context).scan(partition)

            override fun value(): Record {
                /* Read values from underlying columns. */
                val tupleId = this.wrapped.next()
                val values = columns.map {
                    val column = this@EntityV1.columns[it.name]
                        ?: throw IllegalArgumentException("Column $it does not exist on entity ${this@EntityV1.name}.")
                    column.newTx(this@Tx.context).get(tupleId)
                }.toTypedArray()

                /* Return value of all the desired columns. */
                return StandaloneRecord(tupleId, columns, values)
            }
            override fun key(): TupleId = this.wrapped.next()
            override fun moveNext(): Boolean = this.wrapped.hasNext()
            override fun close() { /* No op. */ }
        }

        override fun count(): Long {
            return this@EntityV1.header.size
        }

        override fun insert(record: Record): Record {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }

        override fun update(record: Record) {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }

        override fun delete(tupleId: TupleId) {
            throw UnsupportedOperationException("Operation not supported on legacy DBO.")
        }
    }
}