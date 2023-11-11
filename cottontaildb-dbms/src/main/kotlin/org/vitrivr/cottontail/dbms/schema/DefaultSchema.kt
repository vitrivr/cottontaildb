package org.vitrivr.cottontail.dbms.schema

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Environment
import jetbrains.exodus.env.StoreConfig
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
import org.vitrivr.cottontail.dbms.events.SchemaEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.execution.transactions.AccessMode
import org.vitrivr.cottontail.dbms.execution.transactions.Transaction
import org.vitrivr.cottontail.dbms.index.basic.IndexType
import org.vitrivr.cottontail.dbms.index.hash.BTreeIndexConfig
import org.vitrivr.cottontail.dbms.sequence.DefaultSequence
import org.vitrivr.cottontail.dbms.sequence.Sequence
import java.util.*
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Default [Schema] implementation in Cottontail DB based on JetBrains Xodus
 *
 * @see Schema
 * @see SchemaTx

 * @author Ralph Gasser
 * @version 4.0.0
 */
class DefaultSchema(override val name: Name.SchemaName, override val parent: DefaultCatalogue, private val environment: Environment): Schema {

    /** A [DefaultSchema] belongs to its parent [DefaultCatalogue]. */
    override val catalogue: DefaultCatalogue = this.parent

    /**
     * Creates and returns a new [DefaultSchema.Tx] for the given [CatalogueTx].
     *
     * @param parent The [SchemaTx] to create the [DefaultSchema.Tx] for.
     * @return New [DefaultSchema.Tx]
     */
    override fun newTx(parent: CatalogueTx): DefaultSchema.Tx {
        require(parent is DefaultCatalogue.Tx) { "DefaultSchema.Tx can only be used with DefaultCatalogue.Tx" }
        return this.Tx(parent)
    }

    /**
     * A [Tx] that affects this [DefaultSchema].
     *
     * @author Ralph Gasser
     * @version 3.0.0
     */
    inner class Tx(parent: DefaultCatalogue.Tx): SchemaTx {

        /** Reference to the Cottontail DB [Transaction] object. */
        override val transaction: Transaction = parent.transaction

        /** Reference to the surrounding [DefaultSchema]. */
        override val dbo: DefaultSchema
            get() = this@DefaultSchema

        /** The catalogue-level Xodus [jetbrains.exodus.env.Transaction]. */
        internal val xodusTx: jetbrains.exodus.env.Transaction = parent.xodusTx

        /** A [ReentrantLock] that synchronises access to this [Tx]'s methods. */
        private val txLatch = ReentrantLock()

        /**
         * Returns a list of all [Name.EntityName]s held by this [DefaultSchema].
         *
         * @return [List] of all [Name.EntityName].
         */
        override fun listEntities(): List<Name.EntityName> = this.txLatch.withLock {
            val store = EntityMetadata.store(this@DefaultSchema.catalogue, this.xodusTx)
            val list = mutableListOf<Name.EntityName>()
            store.openCursor(this.xodusTx).use { cursor ->
                if (cursor.getSearchKeyRange(NameBinding.Schema.toEntry(this@DefaultSchema.name)) != null) {
                    do {
                        val name = NameBinding.Entity.fromEntry(cursor.key)
                        if (name.schema() != this@DefaultSchema.name) break
                        list.add(name)
                    } while (cursor.next)
                }
            }
            list
        }

        /**
         * Returns a list of all [Name.SequenceName]s held by this [DefaultSchema].
         *
         * @return [List] of all [Name.SequenceName].
         */
        override fun listSequence(): List<Name.SequenceName> {
            val store = this@DefaultSchema.environment.openStore(DefaultSequence.CATALOGUE_SEQUENCE_STORE_NAME, StoreConfig.USE_EXISTING, this.xodusTx, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open store for sequence catalogue.")
            val list = mutableListOf<Name.SequenceName>()
            store.openCursor(xodusTx).use { cursor ->
                if (cursor.getSearchKeyRange(NameBinding.Schema.toEntry(this@DefaultSchema.name)) != null) {
                    do {
                        val name = NameBinding.Sequence.fromEntry(cursor.key)
                        if (name.schema() != this@DefaultSchema.name) break
                        list.add(name)
                    } while (cursor.next)
                }
            }
            return list
        }

        /**
         * Returns an [Entity] if such an instance exists.
         *
         * @param name Name of the [Entity] to access.
         * @return [Entity]
         */
        override fun entityForName(name: Name.EntityName): Entity = this.txLatch.withLock {
            val store = EntityMetadata.store(this@DefaultSchema.catalogue, this.xodusTx)
            val entryRaw = store.get(xodusTx, NameBinding.Entity.toEntry(name)) ?: throw DatabaseException.EntityDoesNotExistException(name)
            val entry = EntityMetadata.fromEntry(entryRaw)
            return DefaultEntity(name, this@DefaultSchema, this.transaction.manager.environment(entry.handle))
        }

        /**
         * Creates a new [DefaultEntity] in this [DefaultSchema].
         *
         * @param name The name of the [DefaultEntity] that should be created.
         * @param columns The [ColumnDef] of the columns the new [DefaultEntity] should have
         */
        override fun createEntity(name: Name.EntityName,columns: Map<Name.ColumnName,ColumnMetadata>): Entity = this.txLatch.withLock {
            /* Check if there is at least one column. */
            if (columns.isEmpty()) {  throw DatabaseException.NoColumnException(name) }

            /* Check if entity already exists. */
            val entityMetadataStore = this.xodusTx.environment.openStore(DefaultCatalogue.ENTITY_METADATA_STORE_PREFIX, StoreConfig.WITHOUT_DUPLICATES, this.xodusTx)
            if (entityMetadataStore.get(xodusTx, NameBinding.Entity.toEntry(name)) != null) {
                throw DatabaseException.EntityAlreadyExistsException(name)
            }

            /* Write entity catalogue entry. */
            val entry = EntityMetadata(UUID.randomUUID(), System.currentTimeMillis(), System.currentTimeMillis())
            if (!entityMetadataStore.add(this.xodusTx, NameBinding.Entity.toEntry(name), EntityMetadata.toEntry(entry))) {
                throw DatabaseException.EntityAlreadyExistsException(name)
            }

            /* Create store for entity. */
            val environment = this.transaction.manager.createEnvironment(entry.handle)
            val definitions = environment.computeInExclusiveTransaction { tx ->
                environment.openBitmap(name.toString(), StoreConfig.WITHOUT_DUPLICATES, tx)
                val columnMetadataStore = tx.environment.openStore(DefaultCatalogue.SCHEMA_METADATA_STORE_PREFIX, StoreConfig.WITHOUT_DUPLICATES, tx)

                /* Add catalogue entries and stores at column level. */
                 columns.map {
                     if (!columnMetadataStore.add(tx, NameBinding.Column.toEntry(it.key), ColumnMetadata.toEntry(it.value))) {
                        throw DatabaseException.DuplicateColumnException(name, it.key)
                    }
                    ColumnDef(it.key, it.value.type, nullable = it.value.nullable, primary = it.value.primary, autoIncrement = it.value.autoIncrement)
                }.toTypedArray()
            }

            /* Create Event and notify observers */
            val event = EntityEvent.Create(name, definitions)
            this.transaction.signalEvent(event)

            /* Create index for primary key (if defined). */
            val entity = DefaultEntity(name, this@DefaultSchema, this.transaction.manager.environment(entry.handle))
            val primary = definitions.filter { it.primary }.map { it.name }
            if (primary.isNotEmpty()) {
                val entityTx = entity.newTx(this)
                entityTx.createIndex(entity.name.index("primary"), IndexType.BTREE_UQ, primary, BTreeIndexConfig)
            }

            /* Return entity. */
            entity
        }

        /**
         * Drops (i.e., removes) the [DefaultSchema] backed by this [SchemaTx].
         */
        override fun drop() = this.txLatch.withLock {
            /* Check if schema exists! */
            val store = this.xodusTx.environment.openStore(DefaultCatalogue.SCHEMA_METADATA_STORE_PREFIX, StoreConfig.USE_EXISTING, this.xodusTx)
            if (!store.delete(this.xodusTx, NameBinding.Schema.toEntry(this@DefaultSchema.name))) {
                throw DatabaseException.DataCorruptionException("DROP SCHEMA ${this@DefaultSchema.name} failed: Failed to delete catalogue entry.")
            }

            /* Drop all entities. */
            this.listEntities().forEach {
                this.transaction.entityTx(it, AccessMode.WRITE).drop()
            }

            /* Create Event and notify observers */
            val event = SchemaEvent.Drop(name)
            this.transaction.signalEvent(event)
        }

        /**
         * Returns an [Entity] if such an instance exists.
         *
         * @param name Name of the [Entity] to access.
         * @return [Entity] or null.
         */
        override fun sequenceForName(name: Name.SequenceName): Sequence = this.txLatch.withLock {
            val store = this@DefaultSchema.environment.openStore(DefaultSequence.CATALOGUE_SEQUENCE_STORE_NAME, StoreConfig.USE_EXISTING, this.xodusTx, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open store for sequence catalogue.")
            if (store.get(this.xodusTx, NameBinding.Sequence.toEntry(name)) == null) {
                throw DatabaseException.SequenceDoesNotExistException(name)
            }
            return DefaultSequence(name, this@DefaultSchema)
        }

        /**
         * Creates a [DefaultSequence] in the [DefaultSchema] underlying this [DefaultSchema.Tx].
         *
         * @param name The name of the [DefaultSequence] that should be created.
         * @return [DefaultSequence]
         */
        override fun createSequence(name: Name.SequenceName): Sequence = this.txLatch.withLock {
            val store = this@DefaultSchema.environment.openStore(DefaultSequence.CATALOGUE_SEQUENCE_STORE_NAME, StoreConfig.USE_EXISTING, this.xodusTx, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to create store for sequence catalogue.")
            if (!store.put(xodusTx, NameBinding.Sequence.toEntry(name), LongBinding.longToCompressedEntry(0L))) {
                throw DatabaseException.SequenceAlreadyExistsException(name)
            }
            DefaultSequence(name, this@DefaultSchema)
        }

        /**
         * Drops a [DefaultSequence] from the [DefaultSchema] underlying this [DefaultSchema.Tx].
         *
         * @param name The name of the [DefaultSequence] that should be created.
         * @return [DefaultSequence]
         */
        override fun dropSequence(name: Name.SequenceName) = this.txLatch.withLock {
            val store = this@DefaultSchema.environment.openStore(DefaultSequence.CATALOGUE_SEQUENCE_STORE_NAME, StoreConfig.USE_EXISTING, this.xodusTx, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open store for sequence catalogue.")
            if (!store.delete(xodusTx, NameBinding.Sequence.toEntry(name))) {
                throw DatabaseException.SequenceDoesNotExistException(name)
            }
            Unit
        }
    }
}