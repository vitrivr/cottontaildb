package org.vitrivr.cottontail.dbms.entity

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.LongBinding
import jetbrains.exodus.util.ByteArraySizedInputStream
import jetbrains.exodus.util.LightOutputStream
import org.vitrivr.cottontail.dbms.catalogue.Catalogue
import java.io.ByteArrayInputStream
import java.util.*

/**
 * A [EntityMetadata] in the Cottontail DB [Catalogue]. Used to store metadata about [Entity]s.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class EntityMetadata(val handle: UUID = UUID.randomUUID(), val created: Long, val modified: Long) {

    companion object {
        /**
         * De-serializes a [EntityMetadata] from the given [ByteArrayInputStream].
         */
        fun fromEntry(entry: ByteIterable): EntityMetadata {
            val stream = ByteArraySizedInputStream(entry.bytesUnsafe, 0, entry.length)
            val handle = UUID(LongBinding.readCompressed(stream), LongBinding.readCompressed(stream))
            val created = LongBinding.readCompressed(stream)
            val modified = LongBinding.readCompressed(stream)
            return EntityMetadata(handle, created, modified)
        }

        /**
         * Serializes a [EntityMetadata] to the given [ByteIterable].
         *
         * @param entry [EntityMetadata] to serialize.
         * @return [ByteIterable]
         */
        fun toEntry(entry: EntityMetadata): ByteIterable {
            val output = LightOutputStream()
            LongBinding.writeCompressed(output, entry.handle.mostSignificantBits)
            LongBinding.writeCompressed(output, entry.handle.leastSignificantBits)
            LongBinding.writeCompressed(output, entry.created)
            LongBinding.writeCompressed(output, entry.modified)
            return output.asArrayByteIterable()
        }
    }
}