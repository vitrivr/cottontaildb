package org.vitrivr.cottontail.dbms.column

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.core.types.Types
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
data class ColumnMetadata(val type: Types<*>, val compression: Compression, val fixed: Boolean, val nullable: Boolean, val primary: Boolean, val autoIncrement: Boolean, val currentAutoIncrement: Long = 0L) {

    companion object {
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
            val autoIncrement = LongBinding.readCompressed(iterator)
            return ColumnMetadata(type, compression,  bitmap.isBitSet(0), bitmap.isBitSet(1), bitmap.isBitSet(2), bitmap.isBitSet(3), autoIncrement)
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
            if (entry.fixed) bitmap = bitmap.setBit(0)
            if (entry.nullable) bitmap = bitmap.setBit(1)
            if (entry.primary) bitmap = bitmap.setBit(2)
            if (entry.autoIncrement) bitmap = bitmap.setBit(3)
            IntegerBinding.BINDING.writeObject(output, bitmap)
            LongBinding.writeCompressed(output, entry.currentAutoIncrement)
            return output.asArrayByteIterable()
        }
    }
}