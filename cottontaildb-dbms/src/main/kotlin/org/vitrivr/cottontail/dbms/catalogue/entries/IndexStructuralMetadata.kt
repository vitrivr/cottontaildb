package org.vitrivr.cottontail.dbms.catalogue.entries

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.index.basic.AbstractIndex
import org.vitrivr.cottontail.dbms.index.basic.Index


/**
 * [IndexStructuralMetadata] are used to wrap data structures used by certain [Index] structures. Typically, this information
 * is not part of the (more static) catalogue but stored with the [Index] itself.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
abstract class IndexStructuralMetadata: Comparable<IndexStructuralMetadata> {
    companion object {
        /** Name of the [IndexStructuralMetadata] store in the Cottontail DB catalogue. */
        private const val CATALOGUE_INDEX_STRUCT_STORE_NAME: String = "org.vitrivr.cottontail.indexes.structs"

        /**
         * Returns the [Store] for [IndexStructuralMetadata] entries.
         *
         * @param transaction The Xodus [Transaction] to use.
         * @return [Store]
         */
        fun store(transaction: Transaction): Store = transaction.environment.openStore(CATALOGUE_INDEX_STRUCT_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction)

        /**
         * Reads the [IndexStructuralMetadata] for the given [Name.IndexName] from the given [DefaultCatalogue].
         *
         * @param indexTx [AbstractIndex.Tx] to retrieve the [IndexStructuralMetadata] for.
         * @param binding The Xodus [ComparableBinding] to use.
         */
        internal inline fun <reified T: IndexStructuralMetadata> read(indexTx: AbstractIndex.Tx, binding: ComparableBinding): T? {
            val rawEntry = store(indexTx.xodusTx).get(indexTx.xodusTx, NameBinding.Index.toEntry(indexTx.dbo.name))
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
         * Writes the given [IndexStructuralMetadata] to the given [DefaultCatalogue].
         *
         * @param indexTx [AbstractIndex.Tx] to retrieve the [IndexStructuralMetadata] for.
         * @param entry [IndexStructuralMetadata] to write
         * @param binding The Xodus [ComparableBinding] to use.
         * @return True on success, false otherwise.
         */
        internal inline fun <reified T: IndexStructuralMetadata> write(indexTx: AbstractIndex.Tx, entry: T, binding: ComparableBinding): Boolean =
            store(indexTx.xodusTx).put(indexTx.xodusTx, NameBinding.Index.toEntry(indexTx.dbo.name), binding.objectToEntry(entry))

        /**
         * Deletes the [IndexStructuralMetadata] for the given [Name.IndexName] from the given [DefaultCatalogue].
         *
         * @param indexTx [AbstractIndex.Tx] to retrieve the [IndexStructuralMetadata] for.
         * @return True on success, false otherwise.
         */
        internal fun delete(indexTx: AbstractIndex.Tx): Boolean = store(indexTx.xodusTx).delete(indexTx.xodusTx, NameBinding.Index.toEntry(indexTx.dbo.name))
    }
}