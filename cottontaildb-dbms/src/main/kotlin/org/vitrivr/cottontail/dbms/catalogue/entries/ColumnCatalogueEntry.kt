package org.vitrivr.cottontail.dbms.catalogue.entries

import jetbrains.exodus.bindings.*
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.database.ColumnDef
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.values.types.Types
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.column.Column
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import java.io.ByteArrayInputStream

/**
 * A [ColumnCatalogueEntry] in the Cottontail DB [Catalogue]. Used to store metadata about [Column]s
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class ColumnCatalogueEntry(val name: Name.ColumnName, val type: Types<*>, val nullable: Boolean, val primary: Boolean) {
    /**
     * Creates a [ColumnCatalogueEntry] from the provided [ColumnDef].
     *
     * @param [ColumnDef] to convert.
     */
    constructor(def: ColumnDef<*>) : this(def.name, def.type, def.nullable, def.primary)

    /**
     * Creates a [Serialized] version of this [ColumnCatalogueEntry].
     *
     * @return [Serialized]
     */
    private fun toSerialized() = Serialized(this.type, this.nullable, this.primary)

    /**
     * The [Serialized] version of the [ColumnCatalogueEntry]. That entry does not include the [Name.ColumnName]
     */
    private data class Serialized(val type: Types<*>, val nullable: Boolean, val primary: Boolean): Comparable<Serialized> {

        /**
         * Converts this [Serialized] to an actual [ColumnCatalogueEntry].
         *
         * @param name The [Name.ColumnName] this entry belongs to.
         * @return [ColumnCatalogueEntry]
         */
        fun toActual(name: Name.ColumnName) = ColumnCatalogueEntry(name, this.type, this.nullable, this.primary)

        companion object: ComparableBinding() {
            /**
             * De-serializes a [Serialized] from the given [ByteArrayInputStream].
             */
            override fun readObject(stream: ByteArrayInputStream): Serialized = Serialized(
                Types.forOrdinal(IntegerBinding.readCompressed(stream), IntegerBinding.readCompressed(stream)),
                BooleanBinding.BINDING.readObject(stream),
                BooleanBinding.BINDING.readObject(stream),
            )

            /**
             * Serializes a [Serialized] to the given [LightOutputStream].
             */
            override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
                require(`object` is Serialized) { "$`object` cannot be written as column entry." }
                IntegerBinding.writeCompressed(output, `object`.type.ordinal)
                IntegerBinding.writeCompressed(output, `object`.type.logicalSize)
                BooleanBinding.BINDING.writeObject(output, `object`.nullable)
                BooleanBinding.BINDING.writeObject(output, `object`.primary)
            }
        }
        override fun compareTo(other: Serialized): Int = this.type.ordinal.compareTo(other.type.ordinal)
    }


    companion object {

        /** Name of the [ColumnCatalogueEntry] store in the Cottontail DB catalogue. */
        private const val CATALOGUE_COLUMN_STORE_NAME: String = "ctt_cat_columns"

        /**
         * Initializes the store used to store [IndexCatalogueEntry] in Cottontail DB.
         *
         * @param catalogue The [DefaultCatalogue] to initialize.
         * @param transaction The [Transaction] to use.
         */
        internal fun init(catalogue: DefaultCatalogue, transaction: Transaction) {
            catalogue.environment.openStore(CATALOGUE_COLUMN_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction, true)
                ?: throw DatabaseException.DataCorruptionException("Failed to create entity catalogue store.")
        }

        /**
         * Returns the [Store] for [ColumnCatalogueEntry] entries.
         *
         * @param catalogue [DefaultCatalogue] to access [Store] for.
         * @param transaction The Xodus [Transaction] to use.
         * @return [Store]
         */
        internal fun store(catalogue: DefaultCatalogue, transaction: Transaction ): Store =
            catalogue.environment.openStore(CATALOGUE_COLUMN_STORE_NAME, StoreConfig.USE_EXISTING, transaction, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open store for column catalogue.")

        /**
         * Reads the [ColumnCatalogueEntry] for the given [Name.ColumnName] from the given [DefaultCatalogue].
         *
         * @param name [Name.ColumnName] to retrieve the [ColumnCatalogueEntry] for.
         * @param catalogue [DefaultCatalogue] to retrieve [ColumnCatalogueEntry] from.
         * @param transaction The Xodus [Transaction] to use.
         * @return [ColumnCatalogueEntry]
         */
        internal fun read(name: Name.ColumnName, catalogue: DefaultCatalogue, transaction: Transaction): ColumnCatalogueEntry? {
            val rawName = NameBinding.Column.objectToEntry(name)
            val rawEntry = store(catalogue, transaction).get(transaction, rawName)
            return if (rawEntry != null) {
                (Serialized.entryToObject(rawEntry) as Serialized).toActual(name)
            } else {
                null
            }
        }

        /**
         * Writes the given [ColumnCatalogueEntry] to the given [DefaultCatalogue].
         *
         * @param entry [ColumnCatalogueEntry] to write
         * @param catalogue [DefaultCatalogue] to write [ColumnCatalogueEntry] to.
         * @param transaction The Xodus [Transaction] to use.
         * @return True on success, false otherwise.
         */
        internal fun write(entry: ColumnCatalogueEntry, catalogue: DefaultCatalogue, transaction: Transaction): Boolean =
            store(catalogue, transaction).put(transaction, NameBinding.Column.objectToEntry(entry.name), Serialized.objectToEntry(entry.toSerialized()))

        /**
         * Deletes the [ColumnCatalogueEntry] for the given [Name.ColumnName] from the given [DefaultCatalogue].
         *
         * @param name [Name.ColumnName] of the [ColumnCatalogueEntry] that should be deleted.
         * @param catalogue [DefaultCatalogue] to write [ColumnCatalogueEntry] to.
         * @param transaction The Xodus [Transaction] to use.
         * @return True on success, false otherwise.
         */
        internal fun delete(name: Name.ColumnName, catalogue: DefaultCatalogue, transaction: Transaction): Boolean =
            store(catalogue, transaction).delete(transaction, NameBinding.Column.objectToEntry(name))
    }

    /**
     * Converts this [ColumnCatalogueEntry] to a [ColumnDef].
     *
     * @return [ColumnDef] for this [ColumnCatalogueEntry]
     */
    fun toColumnDef(): ColumnDef<*> = ColumnDef(this.name, this.type, this.nullable, this.primary)
}