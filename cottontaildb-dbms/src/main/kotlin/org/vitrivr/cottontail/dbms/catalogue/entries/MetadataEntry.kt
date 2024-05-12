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
        private const val CATALOGUE_METADATA_STORE_NAME: String = "org.vitrivr.cottontail.metadata"

        /** Metadata entry for DB version. */
        const val METADATA_ENTRY_DB_VERSION = "db.version"

        /**
         * Returns the [Store] for [MetadataEntry] entries.
         *
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return [Store]
         */
        fun store(transaction: Transaction): Store
            = transaction.environment.openStore(CATALOGUE_METADATA_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction)

        /**
         * Reads the [MetadataEntry] for the given [Name.ColumnName] from the given [DefaultCatalogue].
         *
         * @param key [String] key to retrieve the [MetadataEntry] for.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return [MetadataEntry]
         */
        fun read(key: String, transaction: Transaction): MetadataEntry? {
            val rawEntry = store(transaction).get(transaction, StringBinding.stringToEntry(key))
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
         * @param transaction The Xodus [Transaction] to use.
         * @return True on success, false otherwise.
         */
        fun write(entry: MetadataEntry, transaction: Transaction): Boolean =
            store(transaction).put(transaction, StringBinding.stringToEntry(entry.key), StringBinding.stringToEntry(entry.value))

        /**
         * Deletes the [MetadataEntry] for the given [Name.ColumnName] from the given [DefaultCatalogue].
         *
         * @param key [String] key of the [MetadataEntry] that should be deleted.
         * @param transaction The Xodus [Transaction] to use.
         * @return True on success, false otherwise.
         */
        fun delete(key: String, transaction: Transaction): Boolean =
            store(transaction).delete(transaction, StringBinding.stringToEntry(key))
    }
}