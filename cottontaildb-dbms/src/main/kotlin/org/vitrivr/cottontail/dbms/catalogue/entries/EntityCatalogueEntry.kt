package org.vitrivr.cottontail.dbms.catalogue.entries

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.entity.Entity
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import java.io.ByteArrayInputStream

/**
 * A [EntityCatalogueEntry] in the Cottontail DB [Catalogue]. Used to store metadata about [Entity]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class EntityCatalogueEntry(val name: Name.EntityName, val created: Long, val columns: List<Name.ColumnName>, val indexes: List<Name.IndexName>) {

    /**
     * Creates a [Serialized] version of this [EntityCatalogueEntry].
     *
     * @return [Serialized]
     */
    private fun toSerialized() = Serialized(this.created, this.columns.map { it.simple }, this.indexes.map { it.simple })

    /**
     * The [Serialized] version of the [EntityCatalogueEntry]. That entry does not include the [Name] objects.
     */
    private data class Serialized(val created: Long, val columns: List<String>, val indexes: List<String>): Comparable<Serialized> {
        fun toActual(name: Name.EntityName) = EntityCatalogueEntry(name, this.created, this.columns.map { name.column(it) }, this.indexes.map { name.index(it) })

        companion object: ComparableBinding() {
            /**
             * De-serializes a [Serialized] from the given [ByteArrayInputStream].
             */
            override fun readObject(stream: ByteArrayInputStream): Comparable<Nothing> {
                val created = LongBinding.readCompressed(stream)
                val columns = (0 until IntegerBinding.readCompressed(stream)).map {
                    StringBinding.BINDING.readObject(stream)
                }
                val indexes = (0 until IntegerBinding.readCompressed(stream)).map {
                    StringBinding.BINDING.readObject(stream)
                }
                return Serialized(created, columns, indexes)
            }

            /**
             * Serializes a [Serialized] to the given [LightOutputStream].
             */
            override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
                require(`object` is Serialized) { "$`object` cannot be written as entity entry." }
                LongBinding.writeCompressed(output, `object`.created)

                /* Write all columns. */
                IntegerBinding.writeCompressed(output,`object`.columns.size)
                for (columnName in `object`.columns) {
                    StringBinding.BINDING.writeObject(output, columnName)
                }

                /* Write all indexes. */
                IntegerBinding.writeCompressed(output,`object`.indexes.size)
                for (indexName in `object`.indexes) {
                    StringBinding.BINDING.writeObject(output, indexName)
                }
            }
        }
        override fun compareTo(other: Serialized): Int = this.created.compareTo(other.created)
    }

    companion object {

        /** Name of the [EntityCatalogueEntry] store in this [DefaultCatalogue]. */
        private const val CATALOGUE_ENTITY_STORE_NAME: String = "ctt_cat_entities"

        /**
         * Initializes the store used to store [EntityCatalogueEntry] in Cottontail DB.
         *
         * @param catalogue The [DefaultCatalogue] to initialize.
         * @param transaction The [Transaction] to use.
         */
        internal fun init(catalogue: DefaultCatalogue, transaction: Transaction) {
            catalogue.environment.openStore(CATALOGUE_ENTITY_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction, true)
                ?: throw DatabaseException.DataCorruptionException("Failed to create entity catalogue.")
        }

        /**
         * Returns the [Store] for [EntityCatalogueEntry] entries.
         *
         * @param catalogue [DefaultCatalogue] to access [Store] for.
         * @param transaction The Xodus [Transaction] to use. If not set, a new [Transaction] will be created.
         * @return [Store]
         */
        internal fun store(catalogue: DefaultCatalogue, transaction: Transaction): Store =
            catalogue.environment.openStore(CATALOGUE_ENTITY_STORE_NAME, StoreConfig.USE_EXISTING, transaction, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open store for entity catalogue.")

        /**
         * Reads the [EntityCatalogueEntry] for the given [Name.EntityName] from the given [DefaultCatalogue].
         *
         * @param name [Name.EntityName] to retrieve the [EntityCatalogueEntry] for.
         * @param catalogue [DefaultCatalogue] to retrieve [EntityCatalogueEntry] from.
         * @param transaction The Xodus [Transaction] to use.
         * @return [EntityCatalogueEntry]
         */
        internal fun read(name: Name.EntityName, catalogue: DefaultCatalogue, transaction: Transaction): EntityCatalogueEntry? {
            val rawEntry = store(catalogue, transaction).get(transaction, NameBinding.Entity.objectToEntry(name))
            return if (rawEntry != null) {
                (Serialized.entryToObject(rawEntry) as Serialized).toActual(name)
            } else {
                null
            }
        }

        /**
         * Checks if the [EntityCatalogueEntry] for the given [Name.EntityName] exists.
         *
         * @param name [Name.EntityName] to check.
         * @param catalogue [DefaultCatalogue] to retrieve [EntityCatalogueEntry] from.
         * @param transaction The Xodus [Transaction] to use.
         * @return true of false
         */
        internal fun exists(name: Name.EntityName, catalogue: DefaultCatalogue, transaction: Transaction): Boolean =
            store(catalogue, transaction).get(transaction, NameBinding.Entity.objectToEntry(name)) != null

        /**
         * Writes the given [EntityCatalogueEntry] to the given [DefaultCatalogue].
         *
         * @param entry [EntityCatalogueEntry] to write
         * @param catalogue [DefaultCatalogue] to write [EntityCatalogueEntry] to.
         * @param transaction The Xodus [Transaction] to use.
         * @return True on success, false otherwise.
         */
        internal fun write(entry: EntityCatalogueEntry, catalogue: DefaultCatalogue, transaction: Transaction): Boolean =
            store(catalogue, transaction).put(transaction, NameBinding.Entity.objectToEntry(entry.name), Serialized.objectToEntry(entry.toSerialized()))

        /**
         * Deletes the [EntityCatalogueEntry] for the given [Name.SchemaName] from the given [DefaultCatalogue].
         *
         * @param name [Name.EntityName] of the [EntityCatalogueEntry] that should be deleted.
         * @param catalogue [DefaultCatalogue] to write [EntityCatalogueEntry] to.
         * @param transaction The Xodus [Transaction] to use.
         * @return True on success, false otherwise.
         */
        internal fun delete(name: Name.EntityName, catalogue: DefaultCatalogue, transaction: Transaction): Boolean =
            store(catalogue, transaction).delete(transaction, NameBinding.Entity.objectToEntry(name))
    }
}