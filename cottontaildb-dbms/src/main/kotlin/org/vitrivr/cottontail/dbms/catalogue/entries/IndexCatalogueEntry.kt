package org.vitrivr.cottontail.dbms.catalogue.entries

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.dbms.index.basic.Index
import org.vitrivr.cottontail.dbms.index.basic.IndexConfig
import org.vitrivr.cottontail.dbms.index.basic.IndexState
import org.vitrivr.cottontail.dbms.index.basic.IndexType
import java.io.ByteArrayInputStream

/**
 * A [IndexCatalogueEntry] in the Cottontail DB [Catalogue]. Used to store metadata about [Index]es.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
data class IndexCatalogueEntry(val name: Name.IndexName, val type: IndexType, val state: IndexState, val columns: List<Name.ColumnName>, val config: IndexConfig<*>) {

    /**
     * Creates a [Serialized] version of this [IndexCatalogueEntry].
     *
     * @return [Serialized]
     */
    private fun toSerialized() = Serialized(this.type, this.state, this.columns.map { it.simple }, this.config)

    /**
     * The [Serialized] version of the [IndexCatalogueEntry]. That entry does not include the [Name] objects.
     */
    private data class Serialized(val type: IndexType, val state: IndexState, val columns: List<String>, val config: IndexConfig<*>): Comparable<Serialized> {

        /**
         * Converts this [Serialized] to an actual [IndexCatalogueEntry].
         *
         * @param name The [Name.IndexName] this entry belongs to.
         * @return [IndexCatalogueEntry]
         */
        fun toActual(name: Name.IndexName) = IndexCatalogueEntry(name, this.type, this.state, this.columns.map { name.entity().column(it) }, this.config)

        companion object: ComparableBinding() {

            /**
             * De-serializes a [Serialized] from the given [ByteArrayInputStream].
             */
            override fun readObject(stream: ByteArrayInputStream): Serialized {
                val type = IndexType.values()[IntegerBinding.readCompressed(stream)]
                val state = IndexState.values()[IntegerBinding.readCompressed(stream)]
                val columns = (0 until IntegerBinding.readCompressed(stream)).map {
                    StringBinding.BINDING.readObject(stream)
                }
                val config = type.descriptor.configBinding().readObject(stream) as IndexConfig<*>
                return Serialized(type, state, columns, config)
            }

            /**
             * Serializes a [Serialized] to the given [LightOutputStream].
             */
            override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
                require(`object` is Serialized) { "$`object` cannot be written as index entry." }
                IntegerBinding.writeCompressed(output, `object`.type.ordinal)
                IntegerBinding.writeCompressed(output, `object`.state.ordinal)

                /* Write all columns. */
                IntegerBinding.writeCompressed(output,`object`.columns.size)
                for (columnName in `object`.columns) {
                    StringBinding.BINDING.writeObject(output, columnName)
                }

                /* Write index configuration. */
                `object`.type.descriptor.configBinding().writeObject(output, `object`.config)
            }
        }
        override fun compareTo(other: Serialized): Int = this.type.ordinal.compareTo(other.type.ordinal)
    }

    companion object {
        /** Name of the [IndexCatalogueEntry] store in the Cottontail DB catalogue. */
        private const val CATALOGUE_INDEX_STORE_NAME: String = "ctt_cat_indexes"

        /**
         * Initializes the store used to store [IndexCatalogueEntry] in Cottontail DB.
         *
         * @param catalogue The [DefaultCatalogue] to initialize.
         * @param transaction The [Transaction] to use.
         */
        internal fun init(catalogue: DefaultCatalogue, transaction: Transaction) {
            catalogue.environment.openStore(CATALOGUE_INDEX_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction, true)
                ?: throw DatabaseException.DataCorruptionException("Failed to create store for index catalogue.")
        }

        /**
         * Returns the [Store] for [IndexCatalogueEntry] entries.
         *
         * @param catalogue [DefaultCatalogue] to retrieve [IndexCatalogueEntry] from.
         * @param transaction The Xodus [Transaction] to use.
         * @return [Store]
         */
        internal fun store(catalogue: DefaultCatalogue, transaction: Transaction): Store =
            catalogue.environment.openStore(CATALOGUE_INDEX_STORE_NAME, StoreConfig.USE_EXISTING, transaction, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open store for index catalogue.")

        /**
         * Reads the [IndexCatalogueEntry] for the given [Name.IndexName] from the given [DefaultCatalogue].
         *
         * @param name [Name.IndexName] to retrieve the [IndexCatalogueEntry] for.
         * @param catalogue [DefaultCatalogue] to retrieve [IndexCatalogueEntry] from.
         * @param transaction The Xodus [Transaction] to use.
         */
        internal fun read(name: Name.IndexName, catalogue: DefaultCatalogue, transaction: Transaction): IndexCatalogueEntry? {
            val rawEntry = store(catalogue, transaction).get(transaction, NameBinding.Index.objectToEntry(name))
            return if (rawEntry != null) {
                (Serialized.entryToObject(rawEntry) as Serialized).toActual(name)
            } else {
                null
            }
        }

        /**
         * Checks if the [IndexCatalogueEntry] for the given [Name.IndexName] exists.
         *
         * @param name [Name.IndexName] to check.
         * @param catalogue [DefaultCatalogue] to retrieve [IndexCatalogueEntry] from.
         * @param transaction The Xodus [Transaction] to use.
         */
        internal fun exists(name: Name.IndexName, catalogue: DefaultCatalogue, transaction: Transaction): Boolean =
            store(catalogue, transaction).get(transaction, NameBinding.Index.objectToEntry(name)) != null

        /**
         * Writes the given [IndexCatalogueEntry] to the given [DefaultCatalogue].
         *
         * @param entry [IndexCatalogueEntry] to write
         * @param catalogue [DefaultCatalogue] to write [IndexCatalogueEntry] to.
         * @param transaction The Xodus [Transaction] to use.
         * @return True on success, false otherwise.
         */
        internal fun write(entry: IndexCatalogueEntry, catalogue: DefaultCatalogue, transaction: Transaction): Boolean =
            store(catalogue, transaction).put(transaction, NameBinding.Index.objectToEntry(entry.name), Serialized.objectToEntry(entry.toSerialized()))

        /**
         * Deletes the [IndexCatalogueEntry] for the given [Name.IndexName] from the given [DefaultCatalogue].
         *
         * @param name [Name.IndexName] of the [IndexCatalogueEntry] that should be deleted.
         * @param catalogue [DefaultCatalogue] to write [IndexCatalogueEntry] to.
         * @param transaction The Xodus [Transaction] to use.
         * @return True on success, false otherwise.
         */
        internal fun delete(name: Name.IndexName, catalogue: DefaultCatalogue, transaction: Transaction): Boolean =
            store(catalogue, transaction).delete(transaction, NameBinding.Index.objectToEntry(name))

        /**
         * Convenience method to update the [IndexState] of this [IndexCatalogueEntry].
         *
         * @param name [Name.IndexName] of the [IndexCatalogueEntry] that should be deleted.
         * @param state
         * @param catalogue [DefaultCatalogue] to write [IndexCatalogueEntry] to.
         * @param state The new [IndexState]
         * @param transaction The Xodus [Transaction] to use.
         * @return True if state was written OR if state did not change, false otherwise.
         */
        internal fun updateState(name: Name.IndexName, catalogue: DefaultCatalogue, state: IndexState, transaction: Transaction): Boolean {
            /* Obtain old entry and compare state. */
            val oldEntry = read(name, catalogue, transaction) ?: throw DatabaseException.DataCorruptionException("Failed to update state for index $name: Could not read catalogue entry for index.")
            return if (oldEntry.state != state) {
                write(oldEntry.copy(state = state), catalogue, transaction)
            } else {
                true
            }
        }
    }
}