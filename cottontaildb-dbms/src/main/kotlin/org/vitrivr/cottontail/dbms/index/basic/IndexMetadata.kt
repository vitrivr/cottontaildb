package org.vitrivr.cottontail.dbms.index.basic

import jetbrains.exodus.ByteIterable
import jetbrains.exodus.bindings.IntegerBinding
import jetbrains.exodus.bindings.StringBinding
import jetbrains.exodus.util.ByteArraySizedInputStream
import jetbrains.exodus.util.LightOutputStream

/**
 * Metadata maintained and stored by Cottontail DB to described [Index]es.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class IndexMetadata(val type: IndexType, val state: IndexState, val columns: List<String>, val config: IndexConfig<*>) {
    companion object {

        /**
         * De-serializes a [IndexMetadata] from the given [ByteIterable].
         *
         * @param entry The [ByteIterable] entry.
         * @return [IndexMetadata]
         */
        fun fromEntry(entry: ByteIterable): IndexMetadata {
            val stream = ByteArraySizedInputStream(entry.bytesUnsafe, 0, entry.length)
            val type = IndexType.values()[IntegerBinding.readCompressed(stream)]
            val state = IndexState.values()[IntegerBinding.readCompressed(stream)]
            val columns = (0 until IntegerBinding.readCompressed(stream)).map {
                StringBinding.BINDING.readObject(stream)
            }
            val config = type.descriptor.configBinding().readObject(stream) as IndexConfig<*>
            return IndexMetadata(type, state, columns, config)
        }

        /**
         * Serializes a [IndexMetadata] to the given [ByteIterable].
         *
         * @param entry [IndexMetadata] to serialize.
         * @return [ByteIterable]
         */
        fun toEntry(entry: IndexMetadata): ByteIterable {
            val output = LightOutputStream()

            IntegerBinding.writeCompressed(output, entry.type.ordinal)
            IntegerBinding.writeCompressed(output, entry.state.ordinal)

            /* Write all columns. */
            IntegerBinding.writeCompressed(output,entry.columns.size)
            for (columnName in entry.columns) {
                StringBinding.BINDING.writeObject(output, columnName)
            }

            /* Write index configuration. */
            entry.type.descriptor.configBinding().writeObject(output, entry.config)
            return output.asArrayByteIterable()
        }
    }
}