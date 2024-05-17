package org.vitrivr.cottontail.dbms.column

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.env.Store
import jetbrains.exodus.env.StoreConfig
import jetbrains.exodus.env.Transaction
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.database.Name
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.utilities.math.BitUtil.isBitSet
import org.vitrivr.cottontail.utilities.math.BitUtil.setBit
import java.io.ByteArrayInputStream

/**
 * A metadata about column as stored in the Cottontail DB catalogue
 *
 * @author Ralph Gasser
 * @version 2.1.0
 */
data class ColumnMetadata(val type: Types<*>, val nullable: Boolean, val primary: Boolean, val autoIncrement: Boolean, val inline: Boolean) {

    companion object {
        /** Name of the [ColumnMetadata] store in the Cottontail DB catalogue. */
        private const val CATALOGUE_COLUMN_STORE_NAME: String = "org.vitrivr.cottontail.columns"

        /**
         * Returns the [Store] used to store [ColumnMetadata].
         *
         * @param transaction The Xodus [Transaction] to use.
         * @return [Store]
         */
        fun store(transaction: Transaction): Store = transaction.environment.openStore(CATALOGUE_COLUMN_STORE_NAME, StoreConfig.WITHOUT_DUPLICATES_WITH_PREFIXING, transaction)

        /**
         * De-serializes a [ColumnMetadata] from the given [ByteIterable].
         *
         * @param entry The [ByteIterable] entry.
         * @return [ByteIterable]
         */
        fun fromEntry(entry: ByteIterable): ColumnMetadata {
            val iterator = ByteArrayInputStream(entry.bytesUnsafe)
            val type = Types.forOrdinal(IntegerBinding.readCompressed(iterator), IntegerBinding.readCompressed(iterator))
            val bitmap = IntegerBinding.BINDING.readObject(iterator)
            return ColumnMetadata(type, bitmap.isBitSet(0), bitmap.isBitSet(1), bitmap.isBitSet(2), bitmap.isBitSet(3))
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

            /* Encode flags in bitmap. */
            var bitmap = 0
            if (entry.nullable) bitmap = bitmap.setBit(0)
            if (entry.primary) bitmap = bitmap.setBit(1)
            if (entry.autoIncrement) bitmap = bitmap.setBit(2)
            if (entry.inline) bitmap = bitmap.setBit(3)
            IntegerBinding.BINDING.writeObject(output, bitmap)
            return output.asArrayByteIterable()
        }

        /**
         * Internal function used to obtain the name of the Xodus store for the given [Name.ColumnName].
         *
         * @return Store name.
         */
        fun Name.ColumnName.storeName(): String = "$CATALOGUE_COLUMN_STORE_NAME.${this.schema}.${this.entity}.${this.column}"
    }
}