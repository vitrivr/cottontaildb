package org.vitrivr.cottontail.dbms.schema

import jetbrains.exodus.env.StoreConfig
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.catalogue.entries.*
import org.vitrivr.cottontail.dbms.catalogue.storeName
import org.vitrivr.cottontail.dbms.entity.DefaultEntity
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.general.AbstractTx
import org.vitrivr.cottontail.dbms.general.DBOVersion
import org.vitrivr.cottontail.dbms.queries.context.QueryContext
import org.vitrivr.cottontail.dbms.statistics.columns.ColumnStatistic
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
            context.txn.cacheTxForDBO(this)
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
            val store = EntityCatalogueEntry.store(this@DefaultSchema.catalogue, this.context.txn.xodusTx)
            val list = mutableListOf<Name.EntityName>()
            val cursor = store.openCursor(this.context.txn.xodusTx)
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
            if (!EntityCatalogueEntry.exists(name, this@DefaultSchema.catalogue, this.context.txn.xodusTx)) {
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
            if (EntityCatalogueEntry.exists(name, this@DefaultSchema.catalogue, this.context.txn.xodusTx)) {
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
            if (!EntityCatalogueEntry.write(EntityCatalogueEntry(name, System.currentTimeMillis(), columns.map { it.name }, emptyList()), this@DefaultSchema.catalogue, this.context.txn.xodusTx)) {
                throw DatabaseException.DataCorruptionException("CREATE entity $name failed: Failed to create catalogue entry.")
            }

            /* Write sequence catalogue entry. */
            if (!SequenceCatalogueEntries.create(name.tid(), this@DefaultSchema.catalogue, this.context.txn.xodusTx)) {
                throw DatabaseException.DataCorruptionException("CREATE entity $name failed: Failed to create sequence entry for tuple ID.")
            }

            /* Add catalogue entries and stores at column level. */
            columns.forEach {
                if (!ColumnCatalogueEntry.write(ColumnCatalogueEntry(it), this@DefaultSchema.catalogue, this.context.txn.xodusTx)) {
                    throw DatabaseException.DataCorruptionException("CREATE entity $name failed: Failed to create column entry for column $it.")
                }

                this@DefaultSchema.catalogue.columnStatistics.updatePersistently(ColumnStatistic(it), this.context.txn.xodusTx)

                if (this@DefaultSchema.catalogue.environment.openStore(it.name.storeName(), StoreConfig.WITHOUT_DUPLICATES, this.context.txn.xodusTx, true) == null) {
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
            val entry = EntityCatalogueEntry.read(name, this@DefaultSchema.catalogue, this.context.txn.xodusTx) ?: throw DatabaseException.EntityDoesNotExistException(name)

            /* Drop all indexes from entity. */
            val entityTx = DefaultEntity(name, this@DefaultSchema).newTx(this.context)
            entry.indexes.forEach { entityTx.dropIndex(it) }

            /* Drop all columns from entity. */
            entry.columns.forEach {
                if (!ColumnCatalogueEntry.delete(it, this@DefaultSchema.catalogue, this.context.txn.xodusTx))
                    throw DatabaseException.DataCorruptionException("DROP entity $name failed: Failed to delete column entry for column $it.")

                /* Delete column statistics entry. */
                this@DefaultSchema.catalogue.columnStatistics.deletePersistently(it, this.context.txn.xodusTx)

                /* Remove store for column. */
                this@DefaultSchema.parent.environment.removeStore(it.storeName(), this.context.txn.xodusTx)
            }

            /* Now remove all catalogue entries related to entity.  */
            if (!EntityCatalogueEntry.delete(name, this@DefaultSchema.catalogue, this.context.txn.xodusTx)) {
                throw DatabaseException.DataCorruptionException("DROP entity $name failed: Failed to delete catalogue entry.")
            }
            if (!SequenceCatalogueEntries.delete(name.tid(), this@DefaultSchema.catalogue, this.context.txn.xodusTx)) {
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
            val entry = EntityCatalogueEntry.read(name, this@DefaultSchema.catalogue, this.context.txn.xodusTx) ?: throw DatabaseException.EntityDoesNotExistException(name)

            /* Clears all indexes. */
            entry.indexes.forEach {
                val idx = IndexCatalogueEntry.read(it, this@DefaultSchema.catalogue, this.context.txn.xodusTx)  ?: throw DatabaseException.IndexDoesNotExistException(it)
                idx.type.descriptor.deinitialize(it, this@DefaultSchema.catalogue, this.context.txn)
                idx.type.descriptor.initialize(it, this@DefaultSchema.catalogue, this.context.txn)
            }

            /* Clears all columns. */
            entry.columns.forEach {
                this@DefaultSchema.catalogue.environment.truncateStore(it.storeName(), this.context.txn.xodusTx)
            }

            /* Resets the tuple ID counter. */
            if (!SequenceCatalogueEntries.reset(name.tid(), this@DefaultSchema.catalogue, this.context.txn.xodusTx)) {
                if (SequenceCatalogueEntries.read(name.tid(), this@DefaultSchema.catalogue, this.context.txn.xodusTx) == null) {
                    throw DatabaseException.DataCorruptionException("DROP entity $name failed: Failed to reset tuple ID sequence entry.")
                }
            }
        }
    }
}