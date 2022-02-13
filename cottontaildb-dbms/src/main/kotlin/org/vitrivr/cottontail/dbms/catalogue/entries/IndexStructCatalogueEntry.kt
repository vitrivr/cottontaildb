package org.vitrivr.cottontail.dbms.catalogue.entries

import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException


/**
 * An [IndexStructCatalogueEntry] in the Cottontail DB [Catalogue].
 *
 * [IndexStructCatalogueEntry] are used to wrap data structures used for certain index structures.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class IndexStructCatalogueEntry(val name: String): Comparable<IndexStructCatalogueEntry> {

    companion object {
        /** Name of the [IndexStructCatalogueEntry] store in the Cottontail DB catalogue. */
        private const val CATALOGUE_INDEX_STRUCT_STORE_NAME: String = "ctt_cat_indexstructs"

        /**
         * Initializes the store used to store [IndexStructCatalogueEntry] in Cottontail DB.
         *
         * @param catalogue The [DefaultCatalogue] to initialize.
         * @param transaction The [Transaction] to use.
         */
        internal fun init(catalogue: DefaultCatalogue, transaction: Transaction = catalogue.environment.beginTransaction()) {
            catalogue.environment.openStore(CATALOGUE_INDEX_STRUCT_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction, true)
                ?: throw DatabaseException.DataCorruptionException("Failed to create entity catalogue store.")
        }
    }
    override fun compareTo(other: IndexStructCatalogueEntry): Int = this.name.compareTo(other.name)
}