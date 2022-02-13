package org.vitrivr.cottontail.dbms.catalogue.entries

import jetbrains.exodus.bindings.ComparableBinding
import jetbrains.exodus.bindings.IntegerBinding
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
import org.vitrivr.cottontail.dbms.statistics.columns.*
import java.io.ByteArrayInputStream

/**
 * A [StatisticsCatalogueEntry] in the Cottontail DB [Catalogue]. Used to store statics about [Column]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class StatisticsCatalogueEntry(val name: Name.ColumnName, val type: Types<*>, val statistics: ValueStatistics<*>)  {
    /**
     * Creates a [StatisticsCatalogueEntry] from the provided [ColumnDef].
     *
     * @param def The [ColumnDef] to convert.
     */
    constructor(def: ColumnDef<*>) : this(def.name, def.type, when(def.type){
        Types.Boolean -> BooleanValueStatistics()
        Types.Byte -> ByteValueStatistics()
        Types.Short -> ShortValueStatistics()
        Types.Date -> DateValueStatistics()
        Types.Double -> DoubleValueStatistics()
        Types.Float -> FloatValueStatistics()
        Types.Int -> IntValueStatistics()
        Types.Long -> LongValueStatistics()
        Types.String -> StringValueStatistics()
        is Types.BooleanVector -> BooleanVectorValueStatistics(def.type as Types.BooleanVector)
        is Types.DoubleVector -> DoubleVectorValueStatistics(def.type as Types.DoubleVector)
        is Types.FloatVector -> FloatVectorValueStatistics(def.type as Types.FloatVector)
        is Types.IntVector -> IntVectorValueStatistics(def.type as Types.IntVector)
        is Types.LongVector -> LongVectorValueStatistics(def.type as Types.LongVector)
        else -> ValueStatistics(def.type)
    })

    /**
     * Creates a [Serialized] version of this [StatisticsCatalogueEntry].
     *
     * @return [Serialized]
     */
    private fun toSerialized() = Serialized(this.type, this.statistics)

    /**
     * The [Serialized] version of the [StatisticsCatalogueEntry]. That entry does not include the [Name.ColumnName].
     */
    private data class Serialized(val type: Types<*>, val statistics: ValueStatistics<*>): Comparable<Serialized> {

        /**
         * Converts this [Serialized] to an actual [StatisticsCatalogueEntry].
         *
         * @param name The [Name.ColumnName] this entry belongs to.
         * @return [StatisticsCatalogueEntry]
         */
        fun toActual(name: Name.ColumnName) = StatisticsCatalogueEntry(name, this.type, this.statistics)

        companion object: ComparableBinding() {
            /**
             * De-serializes a [Serialized] from the given [ByteArrayInputStream].
             */
            override fun readObject(stream: ByteArrayInputStream): Serialized {
                val statistics = ValueStatistics.read(stream)
                return Serialized(statistics.type, statistics)
            }

            /**
             * Serializes a [Serialized] to the given [LightOutputStream].
             */
            override fun writeObject(output: LightOutputStream, `object`: Comparable<Nothing>) {
                require(`object` is Serialized) { "$`object` cannot be written as statistics entry." }
                IntegerBinding.writeCompressed(output, `object`.type.ordinal)
                IntegerBinding.writeCompressed(output, `object`.type.logicalSize)
                ValueStatistics.write(output, `object`.statistics)
            }
        }
        override fun compareTo(other: Serialized): Int = this.type.ordinal.compareTo(other.type.ordinal)
    }


    companion object {

        /** Name of the [StatisticsCatalogueEntry] store in this [DefaultCatalogue]. */
        private const val CATALOGUE_STATISTICS_STORE_NAME: String = "ctt_cat_statistics"

        /**
         * Returns the [Store] for [ColumnCatalogueEntry] entries.
         *
         * @param catalogue [DefaultCatalogue] to access [Store] for.
         * @param transaction The Xodus [Transaction] to use.
         * @return [Store]
         */
        internal fun store(catalogue: DefaultCatalogue, transaction: Transaction): Store =
            catalogue.environment.openStore(CATALOGUE_STATISTICS_STORE_NAME, StoreConfig.USE_EXISTING, transaction, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open store for column statistics catalogue.")

        /**
         * Initializes the store used to store [SchemaCatalogueEntry] in Cottontail DB.
         *
         * @param catalogue The [DefaultCatalogue] to initialize.
         * @param transaction The [Transaction] to use.
         */
        internal fun init(catalogue: DefaultCatalogue, transaction: Transaction) {
            catalogue.environment.openStore(CATALOGUE_STATISTICS_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction, true)
                ?: throw DatabaseException.DataCorruptionException("Failed to create column statistics catalogue store.")
        }

        /**
         * Reads the [StatisticsCatalogueEntry] for the given [Name.ColumnName] from the given [DefaultCatalogue].
         *
         * @param name [Name.ColumnName] to retrieve the [StatisticsCatalogueEntry] for.
         * @param catalogue [DefaultCatalogue] to retrieve [StatisticsCatalogueEntry] from.
         * @param transaction The Xodus [Transaction] to use.
         * @return [StatisticsCatalogueEntry]
         */
        internal fun read(name: Name.ColumnName, catalogue: DefaultCatalogue, transaction: Transaction): StatisticsCatalogueEntry? {
            val rawEntry = store(catalogue, transaction).get(transaction, NameBinding.Column.objectToEntry(name))
            return if (rawEntry != null) {
                (Serialized.entryToObject(rawEntry) as Serialized).toActual(name)
            } else {
                null
            }
        }

        /**
         * Writes the given [StatisticsCatalogueEntry] to the given [DefaultCatalogue].
         *
         * @param entry [StatisticsCatalogueEntry] to write
         * @param catalogue [DefaultCatalogue] to write [StatisticsCatalogueEntry] to.
         * @param transaction The Xodus [Transaction] to use.
         * @return True on success, false otherwise.
         */
        internal fun write(entry: StatisticsCatalogueEntry, catalogue: DefaultCatalogue, transaction: Transaction): Boolean =
             store(catalogue, transaction).put(transaction, NameBinding.Column.objectToEntry(entry.name), Serialized.objectToEntry(entry.toSerialized()))

        /**
         * Deletes the [StatisticsCatalogueEntry] for the given [Name.ColumnName] from the given [DefaultCatalogue].
         *
         * @param name [Name.ColumnName] of the [StatisticsCatalogueEntry] that should be deleted.
         * @param catalogue [DefaultCatalogue] to write [StatisticsCatalogueEntry] to.
         * @param transaction The Xodus [Transaction] to use.
         * @return True on success, false otherwise.
         */
        internal fun delete(name: Name.ColumnName, catalogue: DefaultCatalogue, transaction: Transaction): Boolean =
            store(catalogue, transaction).delete(transaction, NameBinding.Column.objectToEntry(name))
    }
}