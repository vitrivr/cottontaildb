package org.vitrivr.cottontail.storage.serializers.tablets

import jetbrains.exodus.ByteBufferByteIterable
import jetbrains.exodus.ByteIterable
import net.jpountz.lz4.LZ4Compressor
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.Value
import org.vitrivr.cottontail.core.values.tablets.bytebuffer.AbstractByteBufferTablet
import java.nio.ByteBuffer

/**
 * An implementation of a [TabletSerializer] that uses LZ4 compression for values.
 *
 * Potentially faster than [SnappyTabletSerializer], however, performs poorly for data that exhibits a high entropy.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class LZ4TabletSerializer<T: Value>(override val type: Types<T>, val size: Int): TabletSerializer<T> {

    /** The [LZ4Compressor] instance used by this [LZ4TabletSerializer]. */
    private val compressor: LZ4Compressor = TabletSerializer.FACTORY.highCompressor()

    /** The [LZ4Compressor] instance used by this [LZ4TabletSerializer]. */
    private val decompressor = TabletSerializer.FACTORY.fastDecompressor()

    /**
     * Deserializes a [AbstractByteBufferTablet] of type [FloatVectorValue] from the provided [ByteIterable].
     *
     * @param entry The [ByteIterable] to deserialize from.
     * @return The resulting [AbstractByteBufferTablet].
     */
    override fun fromEntry(entry: ByteIterable): AbstractByteBufferTablet<T> {
        /* Transfer data into buffer. */
        val compressed = ByteBuffer.wrap(entry.bytesUnsafe)

        /* Create empty tablet. */
        val tablet = AbstractByteBufferTablet.of(this.size, this.type)

        /* Decompress payload using LZ4. */
        this.decompressor.decompress(compressed, tablet.buffer)
        tablet.buffer.clear()

        /* Return tablet. */
        return tablet
    }

    /**
     * Serializes a [AbstractByteBufferTablet] of type [DoubleVectorValue] to create a [ByteIterable].
     *
     * @param tablet The [AbstractByteBufferTablet] to serialize.
     * @return The resulting [ByteIterable].
     */
    override fun toEntry(tablet: AbstractByteBufferTablet<T>): ByteIterable {
       /* Allocate buffer for compressed data; this is faster because we can hand the buffer directly as ByteBufferByteIterable. */
        val maxLength = this.compressor.maxCompressedLength(tablet.buffer.capacity())
        val buffer = ByteBuffer.allocate(maxLength)

        /* Compare data. */
        this.compressor.compress(tablet.buffer.clear(), buffer)
        tablet.buffer.clear()

        /* Return ByteBufferByteIterable. */
        return ByteBufferByteIterable(buffer.flip())
    }
}