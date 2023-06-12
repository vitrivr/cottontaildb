package org.vitrivr.cottontail.dbms.catalogue.entries

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.schema.Schema
import java.io.ByteArrayInputStream

/**
 * A [SchemaCatalogueEntry] in the Cottontail DB [Catalogue]. Used to store metadata about [Schema]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class SchemaCatalogueEntry(val name: Name.SchemaName): Comparable<SchemaCatalogueEntry> {

    companion object: ComparableBinding() {

        /** Name of the [SchemaCatalogueEntry] store in this [DefaultCatalogue]. */
        private const val CATALOGUE_SCHEMA_STORE_NAME: String = "ctt_cat_schemas"

        /**
         * Initializes the store used to store [SchemaCatalogueEntry] in Cottontail DB.
         *
         * @param catalogue The [DefaultCatalogue] to initialize.
         * @param transaction The [Transaction] to use.
         */
        internal fun init(catalogue: DefaultCatalogue, transaction: Transaction) {
            catalogue.transactionManager.environment.openStore(CATALOGUE_SCHEMA_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction, true)
                ?: throw DatabaseException.DataCorruptionException("Failed to create schema catalogue store.")
        }

        /**
         * Returns the [Store] for [SchemaCatalogueEntry] entries.
         *
         * @param catalogue [DefaultCatalogue] to access [Store] for.
         * @param transaction The Xodus [Transaction] to use.
         * @return [Store]
         */
        internal fun store(catalogue: DefaultCatalogue, transaction: Transaction): Store {
            return catalogue.transactionManager.environment.openStore(CATALOGUE_SCHEMA_STORE_NAME, StoreConfig.USE_EXISTING, transaction, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open store for schema catalogue.")
        }

        /**
         * Reads the [SchemaCatalogueEntry] for the given [Name.SchemaName] from the given [DefaultCatalogue].
         *
         * @param name [Name.SchemaName] to retrieve the [SchemaCatalogueEntry] for.
         * @param catalogue [DefaultCatalogue] to retrieve [SchemaCatalogueEntry] from.
         * @param transaction The Xodus [Transaction] to use.
         * @return [SchemaCatalogueEntry]
         */
        internal fun read(name: Name.SchemaName, catalogue: DefaultCatalogue, transaction: Transaction): SchemaCatalogueEntry? {
            val rawEntry = store(catalogue, transaction).get(transaction, NameBinding.Schema.objectToEntry(name))
            return if (rawEntry != null) {
                entryToObject(rawEntry) as SchemaCatalogueEntry
            } else {
                null
            }
        }

        /**
         * Reads the [SchemaCatalogueEntry] for the given [Name.SchemaName] from the given [DefaultCatalogue].
         *
         * @param name [Name.SchemaName] to retrieve the [SchemaCatalogueEntry] for.
         * @param catalogue [DefaultCatalogue] to retrieve [SchemaCatalogueEntry] from.
         * @param transaction The Xodus [Transaction] to use.
         * @return [SchemaCatalogueEntry]
         */
        internal fun exists(name: Name.SchemaName, catalogue: DefaultCatalogue, transaction: Transaction): Boolean =
            store(catalogue, transaction).get(transaction, NameBinding.Schema.objectToEntry(name)) != null

        /**
         * Writes the given [SchemaCatalogueEntry] to the given [DefaultCatalogue].
         *
         * @param entry [SchemaCatalogueEntry] to write
         * @param catalogue [DefaultCatalogue] to write [SchemaCatalogueEntry] to.
         * @param transaction The Xodus [Transaction] to use.
         * @return True on success, false otherwise.
         */
        internal fun write(entry: SchemaCatalogueEntry, catalogue: DefaultCatalogue, transaction: Transaction ): Boolean =
            store(catalogue, transaction).put(transaction, NameBinding.Schema.objectToEntry(entry.name), objectToEntry(entry))

        /**
         * Deletes the [SchemaCatalogueEntry] for the given [Name.SchemaName] from the given [DefaultCatalogue].
         *
         * @param name [Name.SchemaName] of the [SchemaCatalogueEntry] that should be deleted.
         * @param catalogue [DefaultCatalogue] to write [SchemaCatalogueEntry] to.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return True on success, false otherwise.
         */
        internal fun delete(name: Name.SchemaName, catalogue: DefaultCatalogue, transaction: Transaction): Boolean
            = store(catalogue, transaction).delete(transaction, NameBinding.Schema.objectToEntry(name))

        override fun readObject(stream: ByteArrayInputStream) = SchemaCatalogueEntry(NameBinding.Schema.readObject(stream))
        override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
            require(`object` is SchemaCatalogueEntry) { "$`object` cannot be written as schema entry." }
            NameBinding.Schema.writeObject(output, `object`.name)
        }
    }
    override fun compareTo(other: SchemaCatalogueEntry): Int = this.name.toString().compareTo(other.name.toString())
}