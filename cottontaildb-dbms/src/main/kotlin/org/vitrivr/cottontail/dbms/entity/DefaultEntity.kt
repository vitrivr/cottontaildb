package org.vitrivr.cottontail.dbms.entity

import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import org.vitrivr.cottontail.core.basics.Cursor
import org.vitrivr.cottontail.core.basics.Record
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.database.TupleId
import org.vitrivr.cottontail.core.recordset.StandaloneRecord
import org.vitrivr.cottontail.core.values.IntValue
import org.vitrivr.cottontail.core.values.LongValue
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.core.values.types.Value
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.entries.*
import org.vitrivr.cottontail.dbms.column.Column
import org.vitrivr.cottontail.dbms.column.ColumnTx
import org.vitrivr.cottontail.dbms.column.DefaultColumn
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.exceptions.TransactionException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.general.AbstractTx
import org.vitrivr.cottontail.dbms.general.DBOVersion
import org.vitrivr.cottontail.dbms.index.*
import org.vitrivr.cottontail.dbms.operations.Operation
import org.vitrivr.cottontail.dbms.schema.DefaultSchema
import org.vitrivr.cottontail.dbms.statistics.columns.ValueStatistics
import kotlin.concurrent.withLock

/**
 * The default [Entity] implementation based on JetBrains Xodus.
 *
 * @see Entity
 * @see EntityTx
 *
 * @author Ralph Gasser
 * @version 3.0.0
 */
class DefaultEntity(override val name: Name.EntityName, override val parent: DefaultSchema) : Entity {

    /** A [DefaultEntity] belongs to the same [DefaultCatalogue] as the [DefaultSchema] it belongs to. */
    override val catalogue: DefaultCatalogue
        get() = this.parent.catalogue

    /** The [DBOVersion] of this [DefaultEntity]. */
    override val version: DBOVersion
        get() = DBOVersion.V3_0

    /** Status indicating whether this [DefaultEntity] is open or closed. */
    override val closed: Boolean
        get() = this.parent.closed

    /** The [Name.SequenceName] for this [DefaultEntity]*/
    private val sequenceName: Name.SequenceName = this@DefaultEntity.name.tid()

    /**
     * Creates and returns a new [DefaultEntity.Tx] for the given [TransactionContext].
     *
     * @param context The [TransactionContext] to create the [DefaultEntity.Tx] for.
     * @return New [DefaultEntity.Tx]
     */
    override fun newTx(context: TransactionContext) = this.Tx(context)

    /**
     *
     */
    override fun close() {/* No op. */ }

    /**
     * A [Tx] that affects this [DefaultEntity]. Opening a [DefaultEntity.Tx] will automatically spawn [ColumnTx]
     * and [IndexTx] for every [Column] and [IndexTx] associated with this [DefaultEntity].
     */
    inner class Tx(context: TransactionContext) : AbstractTx(context), EntityTx {
        /** Reference to the surrounding [DefaultEntity]. */
        override val dbo: DefaultEntity
            get() = this@DefaultEntity

        /** Map of [Name.ColumnName] to [Column]. */
        private val columns = Object2ObjectLinkedOpenHashMap<Name.ColumnName,Column<*>>()

        /** Map of [Name.IndexName] to [IndexTx]. */
        private val indexes = Object2ObjectLinkedOpenHashMap<Name.IndexName,Index>()

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

            /* Load entity entry.  */
            val entityEntry = EntityCatalogueEntry.read(this@DefaultEntity.name, this@DefaultEntity.catalogue, this.context.xodusTx)
                ?: throw DatabaseException.DataCorruptionException("Catalogue entry for entity ${this@DefaultEntity.name} is missing.")

            /* Load a (ordered) map of columns. This map can be kept in memory for the duration of the transaction, because Transaction works with a fixed snapshot.  */
            for (c in entityEntry.columns) {
                val columnEntry = ColumnCatalogueEntry.read(c, this@DefaultEntity.catalogue, this.context.xodusTx)
                    ?: throw DatabaseException.DataCorruptionException("Catalogue entry for column $c is missing.")
                this.columns[c] = DefaultColumn(columnEntry.toColumnDef(), this@DefaultEntity)
            }

            /* Load a map of indexes. This map can be kept in memory for the duration of the transaction, because Transaction works with a fixed snapshot.  */
            for (i in entityEntry.indexes) {
                val indexEntry = IndexCatalogueEntry.read(i, this@DefaultEntity.catalogue, this.context.xodusTx)
                    ?: throw DatabaseException.DataCorruptionException("Catalogue entry for index $i is missing.")
                this.indexes[i] = indexEntry.type.descriptor.open(i, this.dbo)
            }
        }

        /**
         * Returns true if the [Entity] underpinning this [EntityTx] contains the given [TupleId] and false otherwise.
         *
         * If this method returns true, then [EntityTx.read] will return a [Record] for [TupleId]. However, if this method
         * returns false, then [EntityTx.read] will throw an exception for that [TupleId].
         *
         * @param tupleId The [TupleId] of the desired entry
         * @return True if entry exists, false otherwise,
         */
        override fun contains(tupleId: TupleId): Boolean = this.txLatch.withLock {
            (this.context.getTx(this.columns.values.first()) as ColumnTx<*>).contains(tupleId)
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
        override fun read(tupleId: TupleId, columns: Array<ColumnDef<*>>): Record = this.txLatch.withLock {
            /* Read values from underlying columns. */
            val values = columns.map {
                (this.context.getTx(this.columns.values.first()) as ColumnTx<*>)
                val tx = this.context.getTx(this.columns[it.name] ?: throw IllegalArgumentException("Column ${it.name} does not exist on entity ${this@DefaultEntity.name}.")) as ColumnTx<*>
                tx.get(tupleId)
            }.toTypedArray()

            /* Return value of all the desired columns. */
            return StandaloneRecord(tupleId, columns, values)
        }

        /**
         * Returns the number of entries in this [DefaultEntity].
         *
         * @return The number of entries in this [DefaultEntity].
         */
        override fun count(): Long = this.txLatch.withLock {
            (this.context.getTx(this.columns.values.first()) as ColumnTx<*>).count()
        }

        /**
         * Returns the maximum tuple ID occupied by entries in this [DefaultEntity].
         *
         * @return The maximum tuple ID occupied by entries in this [DefaultEntity].
         */
        override fun smallestTupleId(): TupleId = this.txLatch.withLock {
            (this.context.getTx(this.columns.values.first()) as ColumnTx<*>).smallestTupleId()
        }

        /**
         * Returns the maximum tuple ID occupied by entries in this [DefaultEntity].
         *
         * @return The maximum tuple ID occupied by entries in this [DefaultEntity].
         */
        override fun largestTupleId(): TupleId = this.txLatch.withLock {
            (this.context.getTx(this.columns.values.first()) as ColumnTx<*>).largestTupleId()
        }

        /**
         * Lists all [Column]s for the [DefaultEntity] associated with this [EntityTx].
         *
         * @return List of all [Column]s.
         */
        override fun listColumns(): List<ColumnDef<*>> = this.txLatch.withLock {
            this.columns.values.map { it.columnDef }
        }

        /**
         * Returns the [ColumnDef] for the specified [Name.ColumnName].
         *
         * @param name The [Name.ColumnName] of the [Column].
         * @return [ColumnDef] of the [Column].
         */
        override fun columnForName(name: Name.ColumnName): Column<*> = this.txLatch.withLock {
            val fqn = this@DefaultEntity.name.column(name.simple)
            this.columns[fqn] ?: throw DatabaseException.ColumnDoesNotExistException(fqn)
        }

        /**
         * Lists all [Name.IndexName] implementations that belong to this [EntityTx].
         *
         * @return List of [Name.IndexName] managed by this [EntityTx]
         */
        override fun listIndexes(): List<Name.IndexName> = this.txLatch.withLock {
            this.indexes.keys.toList()
        }

        /**
         * Lists [Name.IndexName] for all [Index] implementations that belong to this [EntityTx].
         *
         * @return List of [Name.IndexName] managed by this [EntityTx]
         */
        override fun indexForName(name: Name.IndexName): Index = this.txLatch.withLock {
            this.indexes[name] ?: throw DatabaseException.IndexDoesNotExistException(name)
        }

        /**
         * Creates the [Index] with the given settings
         *
         * @param name [Name.IndexName] of the [Index] to create.
         * @param type Type of the [Index] to create.
         * @param columns The list of [columns] to [Index].
         * @param configuration The [IndexConfig] to initialize the [Index] with.
         * @return Newly created [Index] for use in context of this [Tx]
         */
        override fun createIndex(name: Name.IndexName, type: IndexType, columns: List<Name.ColumnName>, configuration: IndexConfig<*>): Index = this.txLatch.withLock {
            val entity = EntityCatalogueEntry.read(this@DefaultEntity.name, this@DefaultEntity.catalogue, this.context.xodusTx) ?:  throw DatabaseException.DataCorruptionException("CREATE index $name failed: Failed to read catalogue entry for entity.")

            /* Checks if index exists. */
            if (IndexCatalogueEntry.exists(name, this@DefaultEntity.catalogue, this.context.xodusTx)) {
                throw DatabaseException.IndexAlreadyExistsException(name)
            }

            /* Create index catalogue entry. */
            val indexEntry = if (this.count() == 0L && type in setOf(IndexType.BTREE_UQ, IndexType.BTREE, IndexType.LUCENE)) {
                IndexCatalogueEntry(name, type, IndexState.CLEAN, columns, configuration)
            } else {
                IndexCatalogueEntry(name, type, IndexState.DIRTY, columns, configuration)
            }
            if (!IndexCatalogueEntry.write(indexEntry, this@DefaultEntity.catalogue, this.context.xodusTx)) {
                throw DatabaseException.DataCorruptionException("CREATE index $name failed: Failed to create catalogue entry.")
            }

            /* Initialize index store entry. */
            if (!type.descriptor.initialize(name, this)) {
                throw DatabaseException.DataCorruptionException("CREATE index $name failed: Failed to initialize store.")
            }

            /* Update entity catalogue entry. */
            EntityCatalogueEntry.write(entity.copy(indexes = (entity.indexes + name)), this@DefaultEntity.catalogue, this.context.xodusTx)

            /* Try to open index. */
            val ret = type.descriptor.open(name, this@DefaultEntity)
            this.indexes[name] = ret
            return ret
        }

        /**
         * Drops the [Index] with the given name.
         *
         * @param name [Name.IndexName] of the [Index] to drop.
         */
        override fun dropIndex(name: Name.IndexName) = this.txLatch.withLock {
            val entity = EntityCatalogueEntry.read(this@DefaultEntity.name, this@DefaultEntity.catalogue, this.context.xodusTx) ?:  throw DatabaseException.DataCorruptionException("CREATE index $name failed: Failed to read catalogue entry for entity.")

            /* Check if index exists. */
            val indexEntry = IndexCatalogueEntry.read(name, this@DefaultEntity.catalogue, this.context.xodusTx)
                ?: throw DatabaseException.IndexDoesNotExistException(name)

            /* De-initialize the index. */
            if (!indexEntry.type.descriptor.deinitialize(name, this)) {
                throw DatabaseException.DataCorruptionException("DROP index $name failed: Failed to de-initialize store.")
            }

            /* Remove index catalogue entry. */
            this.indexes.remove(name)
            if (!IndexCatalogueEntry.delete(name, this@DefaultEntity.catalogue, this.context.xodusTx)) {
                throw DatabaseException.DataCorruptionException("DROP index $name failed: Failed to delete catalogue entry.")
            }

            /* Update entity catalogue entry. */
            EntityCatalogueEntry.write(entity.copy(indexes = (entity.indexes - name)), this@DefaultEntity.catalogue, this.context.xodusTx)
            Unit
        }

        /**
         * Optimizes the [DefaultEntity] underlying this [Tx]. Optimization involves rebuilding of [Index]es and statistics.
         */
        @Suppress("UNCHECKED_CAST")
        override fun optimize() = this.txLatch.withLock {
            val statistics = this.columns.values.map { /* Reset column statistics. */
                val stat = (this.context.getTx(it) as ColumnTx<*>).statistics() as ValueStatistics<Value>
                stat.reset()
                stat
            }

            /* Iterate over all entries and update indexes and statistics. */
            val cursor = this.cursor(this.columns.values.map { it.columnDef }.toTypedArray())
            while (cursor.moveNext()) {
                val value = cursor.value()
                for ((i,c) in value.columns.withIndex()) {
                    statistics[i].insert(value[c])
                }
            }
            cursor.close()

            /* Write new statistics values. */
            this.columns.values.forEachIndexed { i, c ->
                val entry = StatisticsCatalogueEntry.read(c.columnDef.name, this@DefaultEntity.catalogue, this.context.xodusTx)
                    ?: throw DatabaseException.DataCorruptionException("Failed to DELETE value from ${c.columnDef.name}: Reading column statistics failed.")
                StatisticsCatalogueEntry.write(entry = entry.copy(statistics = statistics[i]), catalogue = this@DefaultEntity.catalogue, transaction = this.context.xodusTx)
            }

            /* Rebuild all indexes. */
            for (index in this.indexes.values) {
                (this.context.getTx(index) as IndexTx).rebuild()
            }
        }

        /**
         * Creates and returns a new [Iterator] for this [DefaultEntity.Tx] that returns
         * all [TupleId]s contained within the surrounding [DefaultEntity].
         *
         * <strong>Important:</strong> It remains to the caller to close the [Iterator]
         *
         * @param columns The [ColumnDef]s that should be scanned.
         *
         * @return [Cursor]
         */
        override fun cursor(columns: Array<ColumnDef<*>>): Cursor<Record> = this.txLatch.withLock {
            return cursor(columns, this.smallestTupleId()..this.largestTupleId())
        }

        /**
         * Creates and returns a new [Iterator] for this [DefaultEntity.Tx] that returns all [TupleId]s
         * contained within the surrounding [DefaultEntity] and a certain range.
         *
         * @param columns The [ColumnDef]s that should be scanned.
         * @param partition The [LongRange] specifying the [TupleId]s that should be scanned.
         *
         * @return [Cursor]
         */
        override fun cursor(columns: Array<ColumnDef<*>>, partition: LongRange) = this.txLatch.withLock {
            object : Cursor<Record> {

                /** The wrapped [Cursor] to iterate over columns. */
                private val cursors: Array<Cursor<out Value?>> = Array(columns.size) {
                    (this@Tx.context.getTx(this@Tx.columns[columns[it].name] ?: throw IllegalStateException("Column ${columns[it]} missing in transaction.")) as ColumnTx<*>).cursor(partition)
                }

                /**
                 * Returns the [TupleId] this [Cursor] is currently pointing to.
                 */
                override fun key(): TupleId = this.cursors[0].key()

                /**
                 * Returns the [Record] this [Cursor] is currently pointing to.
                 */
                override fun value(): Record = StandaloneRecord(this.cursors[0].key(), columns, Array(columns.size) { this.cursors[it].value() })

                /**
                 * Tries to move this [Cursor]. Returns true on success and false otherwise.
                 */
                override fun moveNext(): Boolean = this.cursors.all { it.moveNext() }

                /**
                 * Closes this [Cursor].
                 */
                override fun close() = this.cursors.forEach { it.close() }
            }
        }

        /**
         * Insert the provided [Record].
         *
         * @param record The [Record] that should be inserted.
         * @return The ID of the record or null, if nothing was inserted.
         *
         * @throws TransactionException If some of the [Tx] on [Column] or [Index] level caused an error.
         * @throws DatabaseException If a general database error occurs during the insert.
         */
        @Suppress("UNCHECKED_CAST")
        override fun insert(record: Record): TupleId = this.txLatch.withLock {
            /* Execute INSERT on entity level. */
            val nextTupleId = SequenceCatalogueEntries.next(this@DefaultEntity.sequenceName, this@DefaultEntity.catalogue, this.context.xodusTx)
                ?: throw DatabaseException.DataCorruptionException("Sequence entry for entity ${this@DefaultEntity.name} is missing.")

            /* Execute INSERT on column level. */
            val inserts = Object2ObjectArrayMap<ColumnDef<*>, Value>(this.columns.size)
            for (column in this.columns.values) {
                /* Make necessary checks for value. */
                val value = record[column.columnDef]
                inserts[column.columnDef] = when {
                    column.columnDef.autoIncrement -> {
                        val nextValue = SequenceCatalogueEntries.next(this@DefaultEntity.name.sequence(column.name.simple), this@DefaultEntity.catalogue, this.context.xodusTx)
                        check(nextValue != null) { "Failed to generate next value in sequence for column ${column.name}. This is a programmer's error!"}
                        when (column.type) {
                            Types.Int -> IntValue(nextValue)
                            Types.Long -> LongValue(nextValue)
                            else -> throw IllegalStateException("Columns of types ${column.type} do not allow for serial values. This is a programmer's error!")
                        }
                    }
                    column.columnDef.nullable -> value
                    else -> value ?: throw DatabaseException.ValidationException("Cannot INSERT a NULL value into column ${column.columnDef}.")
                }

                /* Perform insert. */
                (this.context.getTx(column) as ColumnTx<Value>).add(nextTupleId, value)
            }

            /* Issue DataChangeEvent.InsertDataChange event and update indexes. */
            val operation = Operation.DataManagementOperation.InsertOperation(this.context.txId, this@DefaultEntity.name, nextTupleId, inserts)
            for (index in this.indexes.values) {
                (this.context.getTx(index) as IndexTx).insert(operation)
            }

            /* Signal event to transaction context. */
            this.context.signalEvent(operation)

            return nextTupleId
        }

        /**
         * Updates the provided [Record] (identified based on its [TupleId]). Columns specified in the [Record] that are not part
         * of the [DefaultEntity] will cause an error!
         *
         * @param record The [Record] that should be updated
         *
         * @throws DatabaseException If an error occurs during the insert.
         */
        @Suppress("UNCHECKED_CAST")
        override fun update(record: Record) = this.txLatch.withLock {
            /* Execute UPDATE on column level. */
            val updates = Object2ObjectArrayMap<ColumnDef<*>, Pair<Value?, Value?>>(record.columns.size)
            for (def in record.columns) {
                val column = this.columns[def.name] ?: throw DatabaseException.ColumnDoesNotExistException(def.name)
                val columnTx = (this.context.getTx(column) as ColumnTx<*>)
                val value = record[def]
                if (value == null && !def.nullable) throw DatabaseException.ValidationException("Record ${record.tupleId} cannot be updated with NULL value for column $def, because column is not nullable.")
                updates[def] = Pair((columnTx as ColumnTx<Value>).update(record.tupleId, value), value) /* Map: ColumnDef -> Pair[Old, New]. */
            }

            /* Issue DataChangeEvent.UpdateDataChangeEvent and update indexes + statistics. */
            val operation = Operation.DataManagementOperation.UpdateOperation(this.context.txId, this@DefaultEntity.name, record.tupleId, updates)
            for (index in this.indexes.values) {
                (this.context.getTx(index) as IndexTx).update(operation)
            }

            /* Signal event to transaction context. */
            this.context.signalEvent(operation)
        }

        /**
         * Deletes the entry with the provided [TupleId].
         *
         * @param tupleId The [TupleId] of the entry that should be deleted.
         *
         * @throws DatabaseException If an error occurs during the insert.
         */
        override fun delete(tupleId: TupleId) = this.txLatch.withLock {
            /* Perform DELETE on column level. */
            val deleted = Object2ObjectArrayMap<ColumnDef<*>, Value?>(this.columns.size)
            for (column in this.columns.values) {
                deleted[column.columnDef] = (this.context.getTx(column) as ColumnTx<*>).delete(tupleId)
            }

            /* Issue DataChangeEvent.DeleteDataChangeEvent and update indexes + statistics. */
            val operation = Operation.DataManagementOperation.DeleteOperation(this.context.txId, this@DefaultEntity.name, tupleId, deleted)
            for (index in this.indexes.values) {
                (this.context.getTx(index) as IndexTx).delete(operation)
            }

            /* Signal event to transaction context. */
            this.context.signalEvent(operation)
        }

        /**
         * Called when a transaction finalizes. Releases the lock held on the [DefaultCatalogue].
         */
        override fun cleanup() {
            this.dbo.catalogue.closeLock.unlockRead(this.closeStamp)
        }

    }
}
