package org.vitrivr.cottontail.storage.serializers.tablets

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.tablets.bytebuffer.AbstractByteBufferTablet

/**
 * An implementation of a [TabletSerializer] that uses no compression of values.
 *
 * Can be a good choice if data cannot be compressed efficiently.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class NoneTabletSerializer<T: Value>(override val type: Types<T>, val size: Int): TabletSerializer<T> {

    /**
     * Deserializes a [AbstractByteBufferTablet] of type [FloatVectorValue] from the provided [ByteIterable].
     *
     * @param entry The [ByteIterable] to deserialize from.
     * @return The resulting [AbstractByteBufferTablet].
     */
    override fun fromEntry(entry: ByteIterable): AbstractByteBufferTablet<T> {
        /* Transfer data into buffer. */
        val tablet = AbstractByteBufferTablet.of(this.size, this.type)
        for (b in entry) {
            tablet.buffer.put(b)
        }
        return tablet
    }

    /**
     * Serializes a [AbstractByteBufferTablet] of type [DoubleVectorValue] to create a [ByteIterable].
     *
     * @param tablet The [AbstractByteBufferTablet] to serialize.
     * @return The resulting [ByteIterable].
     */
    override fun toEntry(tablet: AbstractByteBufferTablet<T>): ByteIterable = ArrayByteIterable(ByteArray(tablet.buffer.clear().limit()) {
        tablet.buffer.get()
    })
}