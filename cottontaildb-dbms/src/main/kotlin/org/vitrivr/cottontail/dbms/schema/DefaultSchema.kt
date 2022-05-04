package org.vitrivr.cottontail.dbms.schema

import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.entries.*
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.column.ColumnTx
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.entity.EntityTx
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.exceptions.TransactionException
import org.vitrivr.cottontail.dbms.execution.transactions.TransactionContext
import org.vitrivr.cottontail.dbms.general.AbstractTx
import org.vitrivr.cottontail.dbms.general.DBOVersion
import org.vitrivr.cottontail.dbms.index.IndexTx
import kotlin.concurrent.withLock

/**
 * Default [Schema] implementation in Cottontail DB based on JetBrains Xodus
 *
 * @see Schema
 * @see SchemaTx

 * @author Ralph Gasser
 * @version 3.0.0
 */
class DefaultSchema(override val name: Name.SchemaName, override val parent: DefaultCatalogue) : Schema {

    /** A [DefaultSchema] belongs to its parent [DefaultCatalogue]. */
    override val catalogue: DefaultCatalogue = this.parent

    /** The [DBOVersion] of this [DefaultSchema]. */
    override val version: DBOVersion
        get() = DBOVersion.V3_0

    /** Flag indicating whether this [DefaultSchema] has been closed. Depends solely on the parent [Catalogue]. */
    override val closed: Boolean
        get() = this.parent.closed

    /**
     * Creates and returns a new [DefaultSchema.Tx] for the given [TransactionContext].
     *
     * @param context The [TransactionContext] to create the [DefaultSchema.Tx] for.
     * @return New [DefaultSchema.Tx]
     */
    override fun newTx(context: TransactionContext) = this.Tx(context)

    /**
     * Closes this [DefaultSchema]
     */
    override fun close() { /* No op. */ }

    /**
     * A [Tx] that affects this [DefaultSchema].
     *
     * @author Ralph Gasser
     * @version 3.0.0
     */
    inner class Tx(context: TransactionContext) : AbstractTx(context), SchemaTx {

        /** Reference to the surrounding [DefaultSchema]. */
        override val dbo: DefaultSchema
            get() = this@DefaultSchema

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
        }

        /**
         * Returns a list of all [Name.EntityName]s held by this [DefaultSchema].
         *
         * @return [List] of all [Name.EntityName].
         */
        override fun listEntities(): List<Name.EntityName> = this.txLatch.withLock {
            val store = EntityCatalogueEntry.store(this@DefaultSchema.catalogue, this.context.xodusTx)
            val list = mutableListOf<Name.EntityName>()
            val cursor = store.openCursor(this.context.xodusTx)
            val ret = cursor.getSearchKeyRange(NameBinding.Schema.objectToEntry(this@DefaultSchema.name)) /* Prefix matching. */
            if (ret != null) {
                do {
                    val name = NameBinding.Entity.entryToObject(cursor.key) as Name.EntityName
                    if (name.schema() != this@DefaultSchema.name) break
                    list.add(name)
                } while (cursor.next)
            }
            cursor.close()
            list
        }

        /**
         * Returns an [Entity] if such an instance exists.
         *
         * @param name Name of the [Entity] to access.
         * @return [Entity] or null.
         */
        override fun entityForName(name: Name.EntityName): Entity = this.txLatch.withLock {
            if (!EntityCatalogueEntry.exists(name, this@DefaultSchema.catalogue, this.context.xodusTx)) {
                throw DatabaseException.EntityDoesNotExistException(name)
            }
            return DefaultEntity(name, this@DefaultSchema)
        }

        /**
         * Creates a new [DefaultEntity] in this [DefaultSchema].
         *
         * @param name The name of the [DefaultEntity] that should be created.
         * @param columns The [ColumnDef] of the columns the new [DefaultEntity] should have
         */
        override fun createEntity(name: Name.EntityName, vararg columns: ColumnDef<*>): Entity = this.txLatch.withLock {
            /* Check if there is at least one column. */
            if (columns.isEmpty()) {
                throw DatabaseException.NoColumnException(name)
            }

            /* Check if entity already exists. */
            if (EntityCatalogueEntry.exists(name, this@DefaultSchema.catalogue, this.context.xodusTx)) {
                throw DatabaseException.EntityAlreadyExistsException(name)
            }

            /* Check if column names are distinct. */
            val distinctSize = columns.map { it.name }.distinct().size
            if (distinctSize != columns.size) {
                columns.forEach { it1 ->
                    if (columns.any { it2 -> it1.name == it2.name && it1 !== it2 }) {
                        throw DatabaseException.DuplicateColumnException(name, it1.name)
                    }
                }
            }

            /* Write entity catalogue entry. */
            if (!EntityCatalogueEntry.write(EntityCatalogueEntry(name, System.currentTimeMillis(), columns.map { it.name }, emptyList()), this@DefaultSchema.catalogue, this.context.xodusTx)) {
                throw DatabaseException.DataCorruptionException("CREATE entity $name failed: Failed to create catalogue entry.")
            }

            /* Write sequence catalogue entry. */
            if (!SequenceCatalogueEntries.create(name.tid(), this@DefaultSchema.catalogue, this.context.xodusTx)) {
                throw DatabaseException.DataCorruptionException("CREATE entity $name failed: Failed to create sequence entry for tuple ID.")
            }

            /* Add catalogue entries and stores at column level. */
            columns.forEach {
                if (!ColumnCatalogueEntry.write(ColumnCatalogueEntry(it), this@DefaultSchema.catalogue, this.context.xodusTx)) {
                    throw DatabaseException.DataCorruptionException("CREATE entity $name failed: Failed to create column entry for column $it.")
                }

                if (!StatisticsCatalogueEntry.write(StatisticsCatalogueEntry(it), this@DefaultSchema.catalogue, this.context.xodusTx)) {
                    throw DatabaseException.DataCorruptionException("CREATE entity $name failed: Failed to create statistics entry for column $it.")
                }

                if (this@DefaultSchema.catalogue.environment.openStore(it.name.storeName(), StoreConfig.WITHOUT_DUPLICATES, this.context.xodusTx, true) == null) {
                    throw DatabaseException.DataCorruptionException("CREATE entity $name failed: Failed to create store for column $it.")
                }
            }

            /* Return a DefaultEntity instance. */
            return DefaultEntity(name, this@DefaultSchema)
        }

        /**
         * Drops an [DefaultEntity] from this [DefaultSchema].
         *
         * @param name The name of the [DefaultEntity] that should be dropped.
         */
        override fun dropEntity(name: Name.EntityName) = this.txLatch.withLock {
            /* Obtain entity entry and thereby check if it exists. */
            val entry = EntityCatalogueEntry.read(name, this@DefaultSchema.catalogue, this.context.xodusTx) ?: throw DatabaseException.EntityDoesNotExistException(name)

            /* Drop all indexes from entity. */
            val entityTx = this.context.getTx(DefaultEntity(name, this@DefaultSchema)) as EntityTx
            entry.indexes.forEach { entityTx.dropIndex(it) }

            /* Drop all columns from entity. */
            entry.columns.forEach {
                if (!ColumnCatalogueEntry.delete(it, this@DefaultSchema.catalogue, this.context.xodusTx))
                    throw DatabaseException.DataCorruptionException("DROP entity $name failed: Failed to delete column entry for column $it.")

                if (!StatisticsCatalogueEntry.delete(it, this@DefaultSchema.catalogue, this.context.xodusTx))
                    throw DatabaseException.DataCorruptionException("DROP entity $name failed: Failed to delete statistics entry for column $it.")

                this@DefaultSchema.parent.environment.removeStore(it.storeName(), this.context.xodusTx)
            }

            /* Now remove all catalogue entries related to entity.  */
            if (!EntityCatalogueEntry.delete(name, this@DefaultSchema.catalogue, this.context.xodusTx)) {
                throw DatabaseException.DataCorruptionException("DROP entity $name failed: Failed to delete catalogue entry.")
            }
            if (!SequenceCatalogueEntries.delete(name.tid(), this@DefaultSchema.catalogue, this.context.xodusTx)) {
                throw DatabaseException.DataCorruptionException("DROP entity $name failed: Failed to delete tuple ID sequence entry.")
            }
        }

        /**
         * Truncates an [Entity] in the [DefaultSchema] underlying this [DefaultSchema.Tx].
         *
         * @param name The name of the [Entity] that should be truncated.
         */
        override fun truncateEntity(name: Name.EntityName) = this.txLatch.withLock {
            /* Obtain entity entry and thereby check if it exists. */
            val entry = EntityCatalogueEntry.read(name, this@DefaultSchema.catalogue, this.context.xodusTx) ?: throw DatabaseException.EntityDoesNotExistException(name)

            /* Clears all indexes. */
            val entityTx = this.context.getTx(DefaultEntity(name, this@DefaultSchema)) as EntityTx
            entry.indexes.forEach {
                val indexTx = this.context.getTx(entityTx.indexForName(it)) as IndexTx
                indexTx.clear()
            }

            /* Clears all columns. */
            entry.columns.forEach {
                val columnTx = this.context.getTx(entityTx.columnForName(it)) as ColumnTx<*>
                columnTx.clear()
            }

            /* Resets the tuple ID counter. */
            if (!SequenceCatalogueEntries.reset(name.tid(), this@DefaultSchema.catalogue, this.context.xodusTx)) {
                throw DatabaseException.DataCorruptionException("DROP entity $name failed: Failed to delete tuple ID sequence entry.")
            }
        }

        /**
         * Called when a transaction finalizes. Releases the lock held on the [DefaultCatalogue].
         */
        override fun cleanup() {
            this.dbo.catalogue.closeLock.unlockRead(this.closeStamp)
        }
    }
}



