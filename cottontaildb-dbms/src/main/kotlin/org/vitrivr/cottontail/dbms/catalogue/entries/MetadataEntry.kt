package org.vitrivr.cottontail.dbms.catalogue.entries

import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException

/**
 * A [MetadataEntry] for Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class MetadataEntry(val key: String, val value: String) {

    companion object {

        /** Name of the [MetadataEntry] store in the Cottontail DB catalogue. */
        private const val CATALOGUE_METADATA_STORE_NAME: String = "ctt_cat_metadata"

        /** Metadata entry for DB version. */
        const val METADATA_ENTRY_DB_VERSION = "db_version"

        /**
         * Initializes the store used to store [MetadataEntry] in Cottontail DB.
         *
         * @param catalogue The [DefaultCatalogue] to initialize.
         * @param transaction The [Transaction] to use.
         */
        internal fun init(catalogue: DefaultCatalogue, transaction: Transaction) {
            catalogue.transactionManager.environment.openStore(CATALOGUE_METADATA_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction, true)
                ?: throw DatabaseException.DataCorruptionException("Failed to create catalogue metadata store.")
        }

        /**
         * Returns the [Store] for [MetadataEntry] entries.
         *
         * @param catalogue [DefaultCatalogue] to access [Store] for.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return [Store]
         */
        internal fun store(catalogue: DefaultCatalogue, transaction: Transaction): Store =
            catalogue.transactionManager.environment.openStore(CATALOGUE_METADATA_STORE_NAME, StoreConfig.USE_EXISTING, transaction, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open catalogue metadata store.")

        /**
         * Reads the [MetadataEntry] for the given [Name.ColumnName] from the given [DefaultCatalogue].
         *
         * @param key [String] key to retrieve the [MetadataEntry] for.
         * @param catalogue [DefaultCatalogue] to retrieve [MetadataEntry] from.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return [MetadataEntry]
         */
        internal fun read(key: String, catalogue: DefaultCatalogue, transaction: Transaction): MetadataEntry? {
            val rawEntry = store(catalogue, transaction).get(transaction, StringBinding.stringToEntry(key))
            return if (rawEntry != null) {
                MetadataEntry(key, StringBinding.entryToString(rawEntry))
            } else {
                null
            }
        }

        /**
         * Writes the given [MetadataEntry] to the given [DefaultCatalogue].
         *
         * @param entry [MetadataEntry] to write
         * @param catalogue [DefaultCatalogue] to write [MetadataEntry] to.
         * @param transaction The Xodus [Transaction] to use.
         * @return True on success, false otherwise.
         */
        internal fun write(entry: MetadataEntry, catalogue: DefaultCatalogue, transaction: Transaction): Boolean =
            store(catalogue, transaction).put(transaction, StringBinding.stringToEntry(entry.key), StringBinding.stringToEntry(entry.value))

        /**
         * Deletes the [MetadataEntry] for the given [Name.ColumnName] from the given [DefaultCatalogue].
         *
         * @param key [String] key of the [MetadataEntry] that should be deleted.
         * @param catalogue [DefaultCatalogue] to write [MetadataEntry] to.
         * @param transaction The Xodus [Transaction] to use.
         * @return True on success, false otherwise.
         */
        internal fun delete(key: String, catalogue: DefaultCatalogue, transaction: Transaction): Boolean =
            store(catalogue, transaction).delete(transaction, StringBinding.stringToEntry(key))
    }
}