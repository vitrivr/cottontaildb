package org.vitrivr.cottontail.dbms.schema

import it.unimi.dsi.fastutil.longs.Long2ObjectFunction
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectLinkedOpenHashMap
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap
import jetbrains.exodus.bindings.LongBinding
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.CatalogueTx
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding
import org.vitrivr.cottontail.dbms.column.ColumnMetadata
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityMetadata
import org.vitrivr.cottontail.dbms.events.EntityEvent
import org.vitrivr.cottontail.dbms.events.Event
import org.vitrivr.cottontail.dbms.events.SequenceEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.SubTransaction
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.sequence.DefaultSequence
import org.vitrivr.cottontail.dbms.sequence.Sequence
import java.util.*

/**
 * Default [Schema] implementation in Cottontail DB based on JetBrains Xodus
 *
 * @see Schema
 * @see SchemaTx

 * @author Ralph Gasser
 * @version 3.1.0
 */
class DefaultSchema(override val name: Name.SchemaName, override val parent: DefaultCatalogue) : Schema {

    /** A [DefaultSchema] belongs to its parent [DefaultCatalogue]. */
    override val catalogue: DefaultCatalogue = this.parent

    /**
     * An internal cache of [Name.EntityName] to [DefaultEntity].
     *
     * These are cached to avoid re-creating them for every query.
     */
    private val entities = Object2ObjectOpenHashMap<Name.EntityName, DefaultEntity>()

    /**
     * An internal cache of [Name.SequenceName] to [DefaultSequence].
     *
     * These are cached to avoid re-creating them for every query.
     */
    private val sequences = Object2ObjectOpenHashMap<Name.SequenceName, DefaultSequence>()

    /** An internal cache of all ongoing [DefaultSchema.Tx]s for this [DefaultSchema]. */
    private val transactions = Long2ObjectOpenHashMap<DefaultSchema.Tx>()

    /**
     * Creates and returns a new [DefaultSchema.Tx] for the given [QueryContext].
     *
     * @param parent The parent [CatalogueTx] object.
     * @return New [DefaultSchema.Tx]
     */
    override fun newTx(parent: CatalogueTx): DefaultSchema.Tx {
        require(parent is DefaultCatalogue.Tx) { "A DefaultSchema can only be accessed with a DefaultSchema.Tx!" }
        return this.transactions.computeIfAbsent(parent.context.txn.transactionId, Long2ObjectFunction {
            val subTransaction = Tx(parent)
            parent.context.txn.registerSubtransaction(subTransaction)
            subTransaction
        })
    }

    /**
     * A [Tx] that affects this [DefaultSchema].
     *
     * @author Ralph Gasser
     * @version 3.0.0
     */
    inner class Tx(override val parent: DefaultCatalogue.Tx): SchemaTx, SubTransaction.WithFinalization {

        /** The Xodus transaction object used by this [DefaultSchema]. */
        internal val xodusTx = this.parent.xodusTx

        init {
            /* Load all entities into cache (done once).  */
            if (this@DefaultSchema.entities.isEmpty()) {
                val store = EntityMetadata.store(this.xodusTx)
                store.openCursor(this.xodusTx).use { cursor ->
                    if (cursor.getSearchKeyRange(NameBinding.Schema.toEntry(this@DefaultSchema.name)) != null) {
                        do {
                            val name = NameBinding.Entity.fromEntry(cursor.key)
                            val metadata = EntityMetadata.fromEntry(cursor.value)
                            if (name.schema() != this@DefaultSchema.name) break
                            this@DefaultSchema.entities[name] = DefaultEntity(name, this@DefaultSchema, metadata.uuid)
                        } while (cursor.next)
                    }
                }
            }

            /* Load all entities into cache (done once).  */
            if (this@DefaultSchema.sequences.isEmpty()) {
                val store = DefaultSequence.store(this.xodusTx)
                store.openCursor(this.xodusTx).use { cursor ->
                    if (cursor.getSearchKeyRange(NameBinding.Schema.toEntry(this@DefaultSchema.name)) != null) {
                        do {
                            val name = NameBinding.Sequence.fromEntry(cursor.key)
                            if (name.schema() != this@DefaultSchema.name) break
                            this@DefaultSchema.sequences[name] = DefaultSequence(name, this@DefaultSchema)
                        } while (cursor.next)
                    }
                }
            }
        }

        /** Reference to the parent [DefaultSchema]. */
        override val dbo: DefaultSchema
            get() = this@DefaultSchema

        /** A [List] of [Event]s that were executed through this [Tx]. */
        private val events = LinkedList<Event>()

        /**
         * A [Object2ObjectLinkedOpenHashMap] of [Entity]s held by this [DefaultSchema.Tx].
         *
         * This starts as a local copy of the [DefaultSchema.entities] but can be modified by this [DefaultSchema.Tx].
         */
        private val entities = Object2ObjectLinkedOpenHashMap(this@DefaultSchema.entities)

        /**
         * A [Object2ObjectLinkedOpenHashMap] of [Sequence]s held by this [DefaultSchema.Tx].
         *
         * This starts as a local copy of the [DefaultSchema.sequences] but can be modified by this [DefaultSchema.Tx].
         */
        private val sequences = Object2ObjectLinkedOpenHashMap(this@DefaultSchema.sequences)

        /**
         * Returns a list of all [Name.EntityName]s held by this [DefaultSchema].
         *
         * @return [List] of all [Name.EntityName].
         */
        @Synchronized
        override fun listEntities(): List<Name.EntityName> {
            return this.entities.keys.toList()
        }

        /**
         * Returns a list of all [Name.SequenceName]s held by this [DefaultSchema].
         *
         * @return [List] of all [Name.SequenceName].
         */
        @Synchronized
        override fun listSequence(): List<Name.SequenceName> {
            return this.sequences.keys.toList()
        }

        /**
         * Returns an [Entity] if such an instance exists.
         *
         * @param name Name of the [Entity] to access.
         * @return [Entity]
         * @throws DatabaseException.EntityDoesNotExistException If the [Entity] with the given [Name.EntityName] does not exist.
         */
        @Synchronized
        override fun entityForName(name: Name.EntityName): Entity {
            return this.entities[name] ?: throw DatabaseException.EntityDoesNotExistException(name)
        }

        /**
         * Returns an [Sequence] if such an instance exists.
         *
         * @param name Name of the [Sequence] to access.
         * @return [Sequence]
         * @throws DatabaseException.SequenceDoesNotExistException If the [Sequence] with the given [Name.SequenceName] does not exist.
         */
        @Synchronized
        override fun sequenceForName(name: Name.SequenceName): Sequence {
            return this.sequences[name] ?: throw DatabaseException.SequenceDoesNotExistException(name)
        }

        /**
         * Creates a new [DefaultEntity] in this [DefaultSchema].
         *
         * @param name The name of the [DefaultEntity] that should be created.
         * @param columns The [ColumnDef] of the columns the new [DefaultEntity] should have
         */
        @Synchronized
        override fun createEntity(name: Name.EntityName, columns: List<Pair<Name.ColumnName, ColumnMetadata>>): Entity {
            /* Check if there is at least one column. */
            if (columns.isEmpty()) {
                throw DatabaseException.NoColumnException(name)
            }

            /* Check if entity already exists. */
            val store = EntityMetadata.store(this.xodusTx)
            val entry = EntityMetadata(UUID.randomUUID(), System.currentTimeMillis(), columns.map { it.first.column })
            if (!store.add(this.xodusTx, NameBinding.Entity.toEntry(name), EntityMetadata.toEntry(entry))) {
                throw DatabaseException.EntityAlreadyExistsException(name)
            }

            /* Add catalogue entries and stores at column level. */
            val columnDefs = mutableListOf<ColumnDef<*>>()
            for ((columnName, metadata) in columns) {
                val metadataStore = ColumnMetadata.store(this.xodusTx)
                if (!metadataStore.add(this.xodusTx, NameBinding.Column.toEntry(columnName), ColumnMetadata.toEntry(metadata))) {
                    throw DatabaseException.DuplicateColumnException(name, columnName)
                }

                /* Create sequence. */
                if (metadata.autoIncrement) {
                    this.createSequence(columnName.autoincrement()!!)
                }

                columnDefs.add(ColumnDef(columnName, metadata.type, primary = metadata.primary, nullable = metadata.nullable, autoIncrement =  metadata.autoIncrement))
            }

            /* Create entity and cache it. */
            val entity = DefaultEntity(name, this@DefaultSchema, entry.uuid)
            this.entities[name] = entity

            /* Create Event and notify observers */
            val event = EntityEvent.Create(entity, columnDefs)
            this.events.add(event)
            this.context.txn.signalEvent(event)

            /* Return a DefaultEntity instance. */
            return entity
        }

        /**
         * Drops an [DefaultEntity] from this [DefaultSchema].
         *
         * @param name The name of the [DefaultEntity] that should be dropped.
         */
        @Synchronized
        override fun dropEntity(name: Name.EntityName) {
            /* Remove entity. */
            val entity = this.entities.remove(name) ?: throw DatabaseException.EntityDoesNotExistException(name)

            /* Drop all indexes from entity. */
            val entityTx = entity.createOrResumeTx(this)

            /* Get metadata store and check if entity exists */
            val entityMetadata = EntityMetadata.store(this.xodusTx)
            if (!entityMetadata.delete(this.xodusTx, NameBinding.Entity.toEntry(name))) {
                throw DatabaseException.EntityDoesNotExistException(name)
            }

            /* Drop all columns from entity. */
            val columnMetadata = ColumnMetadata.store(this.xodusTx)
            val columnDefs = mutableListOf<ColumnDef<*>>()
            entityTx.listColumns().map {
                if (!columnMetadata.delete(this.xodusTx, NameBinding.Column.toEntry(it.name))) {
                    throw DatabaseException.DataCorruptionException("DROP entity $name failed: Failed to delete column entry for column $it.")
                }

                /* Drop sequence. */
                if (it.autoIncrement) {
                    this.dropSequence(this@DefaultSchema.name.sequence("${it.name.entity}_${it.name.column}_auto"))
                }
                columnDefs.add(it)
            }

            /* Create Event and notify observers */
            val event = EntityEvent.Drop(entity, columnDefs)
            this.events.add(event)
            this.context.txn.signalEvent(event)
        }

        /**
         * Creates a [DefaultSequence] in the [DefaultSchema] underlying this [DefaultSchema.Tx].
         *
         * @param name The name of the [DefaultSequence] that should be created.
         * @return [DefaultSequence]
         */
        @Synchronized
        override fun createSequence(name: Name.SequenceName): Sequence {
            val store = DefaultSequence.store(this.xodusTx)
            if (!store.put(this.xodusTx, NameBinding.Sequence.toEntry(name), LongBinding.longToCompressedEntry(0L))) {
                throw DatabaseException.SequenceAlreadyExistsException(name)
            }

            /* Create sequence and cache it. */
            val sequence = DefaultSequence(name, this@DefaultSchema)
            this.sequences[name] = sequence

            /* Create Event and notify observers */
            val event = SequenceEvent.Create(sequence)
            this.events.add(event)
            this.context.txn.signalEvent(event)

            /* Return sequence. */
            return sequence
        }

        /**
         * Drops a [DefaultSequence] from the [DefaultSchema] underlying this [DefaultSchema.Tx].
         *
         * @param name The name of the [DefaultSequence] that should be created.
         * @return [DefaultSequence]
         */
        @Synchronized
        override fun dropSequence(name: Name.SequenceName) {
            val store = DefaultSequence.store(this.xodusTx)
            if (!store.delete(this.xodusTx, NameBinding.Sequence.toEntry(name))) {
                throw DatabaseException.SequenceDoesNotExistException(name)
            }

            /* Remove sequence from local cache. */
            val sequence = this.sequences.remove(name) ?: throw DatabaseException.SequenceDoesNotExistException(name)

            /* Create Event and notify observers */
            val event = SequenceEvent.Drop(sequence)
            this.events.add(event)
            this.context.txn.signalEvent(event)
        }

        /**
         * Finalizes this [DefaultSchema.Tx] and persists all changes to the underlying [DefaultSchema].
         */
        override fun finalize(commit: Boolean) {
            this@DefaultSchema.transactions.remove(this.context.txn.transactionId)
            if (!commit) return
            for (event in this.events) {
                when (event) {
                    is EntityEvent.Create ->  this@DefaultSchema.entities[event.entity.name] = event.entity as DefaultEntity
                    is EntityEvent.Drop ->  this@DefaultSchema.entities.remove(event.entity.name)
                    is SequenceEvent.Create ->  this@DefaultSchema.sequences[event.sequence.name] = event.sequence as DefaultSequence
                    is SequenceEvent.Drop ->  this@DefaultSchema.sequences.remove(event.sequence.name)
                    else -> { /* No op */}
                }
            }
        }
    }
}