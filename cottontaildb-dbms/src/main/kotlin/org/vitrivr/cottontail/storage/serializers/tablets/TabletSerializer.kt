package org.vitrivr.cottontail.storage.serializers.tablets

import jetbrains.exodus.ByteIterable
import net.jpountz.lz4.LZ4Factory
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.tablets.bytebuffer.AbstractByteBufferTablet

/**
 * A serializer for Xodus based [Value] [AbstractByteBufferTablet] serialization and deserialization.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface TabletSerializer<T: Value> {

    companion object {
        /** The [LZ4Factory] instance used. */
        val FACTORY: LZ4Factory = LZ4Factory.fastestInstance()
    }

    /** The [Types] converted by this [TabletSerializer]. */
    val type: Types<T>

    /**
     * Deserializes a [AbstractByteBufferTablet] of type [T] from the provided [ByteIterable].
     *
     * @param entry The [ByteIterable] to deserialize from.
     * @return The resulting [AbstractByteBufferTablet].
     */
    fun fromEntry(entry: ByteIterable): AbstractByteBufferTablet<T>

    /**
     * Serializes a [AbstractByteBufferTablet] of type [T] to create a [ByteIterable].
     *
     * @param tablet The [AbstractByteBufferTablet] to serialize.
     * @return The resulting [ByteIterable].
     */
    fun toEntry(tablet: AbstractByteBufferTablet<T>): ByteIterable
}