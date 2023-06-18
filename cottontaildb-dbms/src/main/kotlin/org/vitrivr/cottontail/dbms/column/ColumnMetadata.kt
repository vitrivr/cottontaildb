package org.vitrivr.cottontail.dbms.column

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.dbms.catalogue.DefaultCatalogue
import org.vitrivr.cottontail.dbms.exceptions.DatabaseException
import org.vitrivr.cottontail.storage.serializers.tablets.Compression
import org.vitrivr.cottontail.utilities.math.BitUtil.isBitSet
import org.vitrivr.cottontail.utilities.math.BitUtil.setBit
import java.io.ByteArrayInputStream

/**
 * A metadata about [Column]s as stored in the Cottontail DB catalogue
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class ColumnMetadata(val type: Types<*>, val compression: Compression, val nullable: Boolean, val primary: Boolean, val autoIncrement: Boolean) {
    companion object {
        /** Name of the [ColumnMetadata] store in the Cottontail DB catalogue. */
        private const val CATALOGUE_COLUMN_STORE_NAME: String = "org.vitrivr.cottontail.columns"

        /**
         * Initializes the store used to store [ColumnMetadata].
         *
         * @param catalogue The [DefaultCatalogue] to initialize.
         * @param transaction The [Transaction] to use.
         */
        internal fun init(catalogue: DefaultCatalogue, transaction: Transaction) {
            catalogue.transactionManager.environment.openStore(CATALOGUE_COLUMN_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction, true)
                ?: throw DatabaseException.DataCorruptionException("Failed to create column catalogue store.")
        }

        /**
         * Returns the [Store] used to store [ColumnMetadata].
         *
         * @param catalogue [DefaultCatalogue] to access [Store] for.
         * @param transaction The Xodus [Transaction] to use.
         * @return [Store]
         */
        internal fun store(catalogue: DefaultCatalogue, transaction: Transaction): Store =
            catalogue.transactionManager.environment.openStore(CATALOGUE_COLUMN_STORE_NAME, StoreConfig.USE_EXISTING, transaction, false)
                ?: throw DatabaseException.DataCorruptionException("Failed to open store for column catalogue.")

        /**
         * De-serializes a [ColumnMetadata] from the given [ByteIterable].
         *
         * @param entry The [ByteIterable] entry.
         * @return [ByteIterable]
         */
        fun fromEntry(entry: ByteIterable): ColumnMetadata {
            val iterator = ByteArrayInputStream(entry.bytesUnsafe)
            val type = Types.forOrdinal(IntegerBinding.readCompressed(iterator), IntegerBinding.readCompressed(iterator))
            val compression = Compression.values()[IntegerBinding.readCompressed(iterator)]
            val bitmap = IntegerBinding.BINDING.readObject(iterator)
            return ColumnMetadata(type, compression,  bitmap.isBitSet(0), bitmap.isBitSet(1), bitmap.isBitSet(2))
        }

        /**
         * Serializes a [ColumnMetadata] to the given [ByteIterable].
         *
         * @param entry [ColumnMetadata] to serialize.
         * @return [ByteIterable]
         */
        fun toEntry(entry: ColumnMetadata): ByteIterable {
            val output = LightOutputStream()
            IntegerBinding.writeCompressed(output, entry.type.ordinal)
            IntegerBinding.writeCompressed(output, entry.type.logicalSize)
            IntegerBinding.writeCompressed(output, entry.compression.ordinal)

            /* Encode flags in bitmap. */
            var bitmap = 0
            if (entry.nullable) bitmap = bitmap.setBit(0)
            if (entry.primary) bitmap = bitmap.setBit(1)
            if (entry.autoIncrement) bitmap = bitmap.setBit(2)
            IntegerBinding.BINDING.writeObject(output, bitmap)
            return output.asArrayByteIterable()
        }
    }
}