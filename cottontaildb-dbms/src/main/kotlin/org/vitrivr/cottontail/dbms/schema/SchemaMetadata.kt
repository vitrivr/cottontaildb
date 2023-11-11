package org.vitrivr.cottontail.dbms.schema

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import java.io.ByteArrayInputStream


/**
 * A [SchemaMetadata] in the Cottontail DB [Catalogue]. Used to store metadata about [Schema]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class SchemaMetadata(val created: Long, val updated: Long) {
    companion object {
        /**
         * De-serializes a [SchemaMetadata] from the given [ByteIterable].
         *
         * @param entry [ByteIterable] to deserialize.
         * @return [SchemaMetadata]
         */
        fun fromEntry(entry: ByteIterable): SchemaMetadata {
            val stream = ByteArrayInputStream(entry.bytesUnsafe, 0, entry.length)
            return SchemaMetadata(LongBinding.readCompressed(stream), LongBinding.readCompressed(stream))
        }

        /**
         * Serializes a [SchemaMetadata] to the given [ByteIterable].
         *
         * @param entry [SchemaMetadata] to serialize.
         * @return [ByteIterable]
         */
        fun toEntry(entry: SchemaMetadata): ByteIterable {
            val output = LightOutputStream()
            LongBinding.writeCompressed(output, entry.created)
            LongBinding.writeCompressed(output, entry.updated)
            return output.asArrayByteIterable()
        }
    }
}