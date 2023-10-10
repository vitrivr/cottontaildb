package org.vitrivr.cottontail.dbms.schema

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.entries.NameBinding
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.column.ColumnMetadata
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityMetadata
import org.vitrivr.cottontail.dbms.events.EntityEvent
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.general.AbstractTx
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.sequence.DefaultSequence
import org.vitrivr.cottontail.dbms.sequence.Sequence
import kotlin.concurrent.withLock

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
     * Creates and returns a new [DefaultSchema.Tx] for the given [QueryContext].
     *
     * @param context The [QueryContext] to create the [DefaultSchema.Tx] for.
     * @return New [DefaultSchema.Tx]
     */
    override fun newTx(context: QueryContext): SchemaTx
        = context.txn.getCachedTxForDBO(this) ?: this.Tx(context)

    /**
     * A [Tx] that affects this [DefaultSchema].
     *
     * @author Ralph Gasser
     * @version 3.0.0
     */
    inner class Tx(context: QueryContext) : AbstractTx(context), SchemaTx {

        init {
            /* Cache this Tx for future use. */
            context.txn.cacheTx(this)
        }

        /** Reference to the surrounding [DefaultSchema]. */
        override val dbo: DefaultSchema
            get() = this@DefaultSchema

        /**
         * Returns a list of all [Name.EntityName]s held by this [DefaultSchema].
         *
         * @return [List] of all [Name.EntityName].
         */
        override fun listEntities(): List<Name.EntityName> = this.txLatch.withLock {
            val store = EntityMetadata.store(this@DefaultSchema.catalogue, this.context.txn.xodusTx)
            val list = mutableListOf<Name.EntityName>()
            store.openCursor(this.context.txn.xodusTx).use { cursor ->
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
            val store = DefaultSequence.store(this@DefaultSchema.catalogue, this.context.txn.xodusTx)
            val list = mutableListOf<Name.SequenceName>()
            store.openCursor(this.context.txn.xodusTx).use { cursor ->
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
            val store = EntityMetadata.store(this@DefaultSchema.catalogue, this.context.txn.xodusTx)
            if (store.get(this.context.txn.xodusTx, NameBinding.Entity.toEntry(name)) == null) {
                throw DatabaseException.EntityDoesNotExistException(name)
            }
            return DefaultEntity(name, this@DefaultSchema)
        }

        /**
         * Returns an [Entity] if such an instance exists.
         *
         * @param name Name of the [Entity] to access.
         * @return [Entity] or null.
         */
        override fun sequenceForName(name: Name.SequenceName): Sequence = this.txLatch.withLock {
            val store = DefaultSequence.store(this@DefaultSchema.catalogue, this.context.txn.xodusTx)
            if (store.get(this.context.txn.xodusTx, NameBinding.Sequence.toEntry(name)) == null) {
                throw DatabaseException.SequenceDoesNotExistException(name)
            }
            return DefaultSequence(name, this@DefaultSchema)
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
            val store = EntityMetadata.store(this@DefaultSchema.catalogue, this.context.txn.xodusTx)
            if (store.get(this.context.txn.xodusTx, NameBinding.Entity.toEntry(name)) != null) {
                throw DatabaseException.EntityAlreadyExistsException(name)
            }

            /* Write entity catalogue entry. */
            val entry = EntityMetadata(System.currentTimeMillis(), columns.keys.map { it.columnName }, emptyList())
            if (!store.add(this.context.txn.xodusTx, NameBinding.Entity.toEntry(name), EntityMetadata.toEntry(entry))) {
                throw DatabaseException.EntityAlreadyExistsException(name)
            }

            /* Create bitmap store for entity. */
            this@DefaultSchema.catalogue.transactionManager.environment.openBitmap(name.toString(), StoreConfig.WITHOUT_DUPLICATES, this.context.txn.xodusTx)

            /* Add catalogue entries and stores at column level. */
            val definitions = columns.map {
                val metadataStore = ColumnMetadata.store(this@DefaultSchema.catalogue, this.context.txn.xodusTx)
                if (!metadataStore.add(this.context.txn.xodusTx, NameBinding.Column.toEntry(it.key), ColumnMetadata.toEntry(it.value))) {
                    throw DatabaseException.DuplicateColumnException(name, it.key)
                }

                /* Create sequence. */
                if (it.value.autoIncrement) {
                    this.createSequence(this@DefaultSchema.name.sequence("${it.key.entityName}_${it.key.columnName}_auto"))
                }

                /* Create store for column data. */
                if (this@DefaultSchema.catalogue.transactionManager.environment.openStore(it.key.storeName(), StoreConfig.WITHOUT_DUPLICATES, this.context.txn.xodusTx, true) == null) {
                    throw DatabaseException.DataCorruptionException("CREATE entity $name failed: Failed to create store for column $it.")
                }

                ColumnDef(it.key, it.value.type, nullable = it.value.nullable, primary = it.value.primary, autoIncrement = it.value.autoIncrement)
            }.toTypedArray()

            /* Create Event and notify observers */
            val event = EntityEvent.Create(name, definitions)
            this.context.txn.signalEvent(event)

            /* Return a DefaultEntity instance. */
            return DefaultEntity(name, this@DefaultSchema)
        }

        /**
         * Drops an [DefaultEntity] from this [DefaultSchema].
         *
         * @param name The name of the [DefaultEntity] that should be dropped.
         */
        override fun dropEntity(name: Name.EntityName) = this.txLatch.withLock {
            /* Drop all indexes from entity. */
            val entityTx = DefaultEntity(name, this@DefaultSchema).newTx(this.context)
            entityTx.listIndexes().forEach { entityTx.dropIndex(it) }

            /* Drop all columns from entity. */
            val dropped = entityTx.listColumns().map {
                val metadataStore = ColumnMetadata.store(this@DefaultSchema.catalogue, this.context.txn.xodusTx)

                if (!metadataStore.delete(this.context.txn.xodusTx, NameBinding.Column.toEntry(it.name))) {
                    throw DatabaseException.DataCorruptionException("DROP entity $name failed: Failed to delete column entry for column $it.")
                }

                /* Drop sequence. */
                if (it.autoIncrement) {
                    this.dropSequence(this@DefaultSchema.name.sequence("${it.name.entityName}_${it.name.columnName}_auto"))
                }

                /* Remove store for column. */
                this@DefaultSchema.catalogue.transactionManager.environment.removeStore(it.name.storeName(), this.context.txn.xodusTx)
                it
            }

            /* Now remove all catalogue entries related to entity.  */
            val metadataStore = EntityMetadata.store(this@DefaultSchema.catalogue, this.context.txn.xodusTx)
            if (!metadataStore.delete(this.context.txn.xodusTx, NameBinding.Entity.toEntry(name))) {
                throw DatabaseException.DataCorruptionException("DROP entity $name failed: Failed to delete catalogue entry.")
            }

            /* Drop bitmap store for entity. */
            this@DefaultSchema.catalogue.transactionManager.environment.removeStore("${name}#bitmap", this.context.txn.xodusTx)

            /* Create Event and notify observers */
            val event = EntityEvent.Drop(name, dropped.toTypedArray())
            this.context.txn.signalEvent(event)
        }

        /**
         * Truncates an [Entity] in the [DefaultSchema] underlying this [DefaultSchema.Tx].
         *
         * @param name The name of the [Entity] that should be truncated.
         */
        override fun truncateEntity(name: Name.EntityName) = this.txLatch.withLock {
            /* Reset associated columns & sequences. */
            val entityTx = this.entityForName(name).newTx(this.context)
            entityTx.listColumns().forEach {
                this@DefaultSchema.catalogue.transactionManager.environment.truncateStore(it.name.storeName(), this.context.txn.xodusTx)
                if (it.autoIncrement) {
                    val sequenceTx = this.sequenceForName(this@DefaultSchema.name.sequence("${it.name.entityName}_${it.name.columnName}_auto")).newTx(this.context)
                    sequenceTx.reset()
                }
            }

            /* Reset associated indexes. */
            entityTx.listIndexes().forEach {
                val indexTx = entityTx.indexForName(it).newTx(this.context)
                indexTx.dbo.type.descriptor.deinitialize(it, this@DefaultSchema.catalogue, this.context.txn)
                indexTx.dbo.type.descriptor.initialize(it, this@DefaultSchema.catalogue, this.context.txn)
            }
        }

        /**
         * Creates a [DefaultSequence] in the [DefaultSchema] underlying this [DefaultSchema.Tx].
         *
         * @param name The name of the [DefaultSequence] that should be created.
         * @return [DefaultSequence]
         */
        override fun createSequence(name: Name.SequenceName): Sequence = this.txLatch.withLock {
            val store = DefaultSequence.store(this@DefaultSchema.catalogue, this.context.txn.xodusTx)
            if (!store.put(this.context.txn.xodusTx, NameBinding.Sequence.toEntry(name), LongBinding.longToCompressedEntry(0L))) {
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
            val store = DefaultSequence.store(this@DefaultSchema.catalogue, this.context.txn.xodusTx)
            if (!store.delete(this.context.txn.xodusTx, NameBinding.Sequence.toEntry(name))) {
                throw DatabaseException.SequenceDoesNotExistException(name)
            }
            Unit
        }
    }
}