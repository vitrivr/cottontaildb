package org.vitrivr.cottontail.storage.serializers.tablets

import jetbrains.exodus.ArrayByteIterable
import jetbrains.exodus.ByteIterable
import net.jpountz.lz4.LZ4Compressor
import org.vitrivr.cottontail.core.types.Types
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.DoubleVectorValue
import org.vitrivr.cottontail.core.values.FloatVectorValue
import org.vitrivr.cottontail.core.values.tablets.AbstractTablet
import org.vitrivr.cottontail.core.values.tablets.Tablet
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

    /** Internal buffer used to store compressed data. */
    private val compressBuffer: ByteBuffer = ByteBuffer.allocate(this.compressor.maxCompressedLength( this.size.shr(3) + when(type) {
        Types.Boolean -> this.size.shl(3)
        is Types.BooleanVector -> this.size * this.type.logicalSize.shr(3) + Int.SIZE_BYTES
        else -> this.size * this.type.physicalSize
    }))

    /**
     * Deserializes a [AbstractTablet] of type [FloatVectorValue] from the provided [ByteIterable].
     *
     * @param entry The [ByteIterable] to deserialize from.
     * @return The resulting [AbstractTablet].
     */
    override fun fromEntry(entry: ByteIterable): Tablet<T> {
        /* Transfer data into buffer. */
        this.compressBuffer.clear()
        for (b in entry) {
            this.compressBuffer.put(b)
        }

        /* Decompress payload using LZ4. */
        val tablet = Tablet.of(this.size, this.type)
        this.decompressor.decompress(this.compressBuffer.flip(), tablet.buffer.clear())
        tablet.buffer.clear()
        return tablet
    }

    /**
     * Serializes a [AbstractTablet] of type [DoubleVectorValue] to create a [ByteIterable].
     *
     * @param tablet The [AbstractTablet] to serialize.
     * @return The resulting [ByteIterable].
     */
    override fun toEntry(tablet: Tablet<T>): ByteIterable {
       /* Compress shuffled data and return it as byte array. */
        this.compressor.compress(tablet.buffer.clear(), this.compressBuffer.clear())
        tablet.buffer.clear()
        return ArrayByteIterable(ByteArray(this.compressBuffer.flip().limit()) {
            this.compressBuffer.get()
        })
    }
}