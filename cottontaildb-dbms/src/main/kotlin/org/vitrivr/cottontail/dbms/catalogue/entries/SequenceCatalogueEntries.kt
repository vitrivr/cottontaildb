package org.vitrivr.cottontail.dbms.catalogue.entries

import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException

/**
 * Class used to manipulate sequences in Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
object SequenceCatalogueEntries {
    /** Name of the [Sequence] entry this [DefaultCatalogue]. */
    private const val CATALOGUE_SEQUENCE_STORE_NAME: String = "ctt_cat_sequences"

    /**
     * Initializes the store used to store sequences in Cottontail DB.
     */
    internal fun init(catalogue: DefaultCatalogue, transaction: Transaction) {
        catalogue.transactionManager.environment.openStore(CATALOGUE_SEQUENCE_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction, true)
            ?: throw DatabaseException.DataCorruptionException("Failed to create store for sequence catalogue.")
    }

    /**
     * Returns the [Store] for [EntityCatalogueEntry] entries.
     *
     * @param catalogue [DefaultCatalogue] to retrieve [EntityCatalogueEntry] from.
     * @param transaction The Xodus [Transaction] to use.
     * @return [EntityCatalogueEntry]
     */
    internal fun store(catalogue: DefaultCatalogue, transaction: Transaction): Store {
        return catalogue.transactionManager.environment.openStore(CATALOGUE_SEQUENCE_STORE_NAME, StoreConfig.USE_EXISTING, transaction, false)
            ?: throw DatabaseException.DataCorruptionException("Failed to open store for sequence catalogue.")
    }

    /**
     * Reads and returns the entry for the given name without changing it.
     *
     * @param name [Name.SequenceName] that identifies the sequence entry.
     * @param catalogue [DefaultCatalogue] to retrieve the sequence entry from.
     * @param transaction The Xodus [Transaction] to use.
     * @return [EntityCatalogueEntry]
     */
    internal fun read(name: Name.SequenceName, catalogue: DefaultCatalogue, transaction: Transaction): Long? {
        val rawEntry = store(catalogue, transaction).get(transaction, NameBinding.Sequence.objectToEntry(name))
        return if (rawEntry != null) {
            LongBinding.compressedEntryToLong(rawEntry)
        } else {
            null
        }
    }

    /** Reads, increments and returns the entry for the given name without changing it.
     *
     * @param name [Name.SequenceName] that identifies the sequence entry.
     * @param catalogue [DefaultCatalogue] to retrieve the sequence entry from.
     * @param transaction The Xodus [Transaction] to use.
     * @return [EntityCatalogueEntry]
     */
    internal fun next(name: Name.SequenceName, catalogue: DefaultCatalogue, transaction: Transaction): Long? {
        val store = store(catalogue, transaction)
        val rawName = NameBinding.Sequence.objectToEntry(name)
        val rawEntry = store(catalogue, transaction).get(transaction, rawName)
        return if (rawEntry != null) {
            val next = LongBinding.compressedEntryToLong(rawEntry) + 1
            store.put(transaction, rawName, LongBinding.longToCompressedEntry(next))
            return next
        } else {
            null
        }
    }

    /**
     * Creates the sequence with the given name.
     *
     * @param name [Name.SequenceName] identifying the sequence to create.
     * @param catalogue [DefaultCatalogue] to reset the sequence.
     * @param transaction The Xodus [Transaction] to use.
     * @return True on success.
     */
    internal fun create(name: Name.SequenceName, catalogue: DefaultCatalogue, transaction: Transaction): Boolean =
        store(catalogue, transaction).add(transaction, NameBinding.Sequence.objectToEntry(name), LongBinding.longToCompressedEntry(0L))

    /**
     * Deletes the sequence with the given name.
     *
     * @param name [Name.SequenceName] identifying the sequence to delete.
     * @param catalogue [DefaultCatalogue] to reset the sequence.
     * @param transaction The Xodus [Transaction] to use.
     * @return True on success.
     */
    internal fun delete(name: Name.SequenceName, catalogue: DefaultCatalogue, transaction: Transaction): Boolean =
        store(catalogue, transaction).delete(transaction, NameBinding.Sequence.objectToEntry(name))

    /**
     * Resets the sequence with the given name.
     *
     * @param name [Name.SequenceName] identifying the sequence to reset.
     * @param catalogue [DefaultCatalogue] to reset the sequence.
     * @param transaction The Xodus [Transaction] to use.
     * @return True on success.
     */
    internal fun reset(name: Name.SequenceName, catalogue: DefaultCatalogue, transaction: Transaction): Boolean =
        store(catalogue, transaction).put(transaction, NameBinding.Sequence.objectToEntry(name), LongBinding.longToCompressedEntry(0L))
}