package org.vitrivr.cottontail.dbms.catalogue.entries

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException


/**
 * An [IndexStructCatalogueEntry] in the Cottontail DB [Catalogue].
 *
 * [IndexStructCatalogueEntry] are used to wrap data structures used for certain index structures.
 *
 * @author Ralph Gasser
 * @version 1.0.1
 */
abstract class IndexStructCatalogueEntry: Comparable<IndexStructCatalogueEntry> {
    companion object {
        /** Name of the [IndexStructCatalogueEntry] store in the Cottontail DB catalogue. */
        private const val CATALOGUE_INDEX_STRUCT_STORE_NAME: String = "ctt_cat_indexstructs"

        /**
         * Initializes the store used to store [IndexStructCatalogueEntry] in Cottontail DB.
         *
         * @param catalogue The [DefaultCatalogue] to initialize.
         * @param transaction The [Transaction] to use.
         */
        internal fun init(catalogue: DefaultCatalogue, transaction: Transaction = catalogue.transactionManager.environment.beginTransaction()) {
            catalogue.transactionManager.environment.openStore(CATALOGUE_INDEX_STRUCT_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction, true)
                ?: throw DatabaseException.DataCorruptionException("Failed to create store for index related data structures.")
        }

        /**
         * Returns the [Store] for [IndexStructCatalogueEntry] entries.
         *
         * @param catalogue [DefaultCatalogue] to retrieve [IndexStructCatalogueEntry] from.
         * @param transaction The Xodus [Transaction] to use.
         * @return [Store]
         */
        internal fun store(catalogue: DefaultCatalogue, transaction: Transaction): Store =
            catalogue.transactionManager.environment.openStore(CATALOGUE_INDEX_STRUCT_STORE_NAME, StoreConfig.USE_EXISTING, transaction, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open store for index related data structures.")

        /**
         * Reads the [IndexStructCatalogueEntry] for the given [Name.IndexName] from the given [DefaultCatalogue].
         *
         * @param name [Name.IndexName] to retrieve the [IndexStructCatalogueEntry] for.
         * @param catalogue [DefaultCatalogue] to retrieve [IndexStructCatalogueEntry] from.
         * @param transaction The Xodus [Transaction] to use.
         */
        internal inline fun <reified T: IndexStructCatalogueEntry> read(name: Name.IndexName, catalogue: DefaultCatalogue, transaction: Transaction, binding: ComparableBinding): T? {
            val rawEntry = store(catalogue, transaction).get(transaction, NameBinding.Index.toEntry(name))
            return if (rawEntry != null) {
                val value = binding.entryToObject(rawEntry)
                if (value is T) {
                    return value
                } else {
                    throw IllegalArgumentException("")
                }
            } else {
                null
            }
        }

        /**
         * Checks if the [IndexStructCatalogueEntry] for the given [Name.IndexName] exists.
         *
         * @param name [Name.IndexName] to check.
         * @param catalogue [DefaultCatalogue] to retrieve [IndexStructCatalogueEntry] from.
         * @param transaction The Xodus [Transaction] to use.
         */
        internal fun exists(name: Name.IndexName, catalogue: DefaultCatalogue, transaction: Transaction): Boolean =
            store(catalogue, transaction).get(transaction, NameBinding.Index.toEntry(name)) != null

        /**
         * Writes the given [IndexStructCatalogueEntry] to the given [DefaultCatalogue].
         *
         * @param entry [IndexStructCatalogueEntry] to write
         * @param catalogue [DefaultCatalogue] to write [IndexStructCatalogueEntry] to.
         * @param transaction The Xodus [Transaction] to use.
         * @return True on success, false otherwise.
         */
        internal inline fun <reified T: IndexStructCatalogueEntry> write(name: Name.IndexName, entry: T, catalogue: DefaultCatalogue, transaction: Transaction, binding: ComparableBinding): Boolean =
            store(catalogue, transaction).put(transaction, NameBinding.Index.toEntry(name), binding.objectToEntry(entry))

        /**
         * Deletes the [IndexStructCatalogueEntry] for the given [Name.IndexName] from the given [DefaultCatalogue].
         *
         * @param name [Name.IndexName] of the [IndexStructCatalogueEntry] that should be deleted.
         * @param catalogue [DefaultCatalogue] to write [IndexStructCatalogueEntry] to.
         * @param transaction The Xodus [Transaction] to use.
         * @return True on success, false otherwise.
         */
        internal fun delete(name: Name.IndexName, catalogue: DefaultCatalogue, transaction: Transaction): Boolean =
            store(catalogue, transaction).delete(transaction, NameBinding.Index.toEntry(name))
    }
}